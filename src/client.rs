use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use labrpc::*;

use crate::msg::*;
use crate::service::{TSOClient, TransactionClient};

/// Base wait (ms) before retrying a failed RPC. Multiplied exponentially per attempt:
/// attempt 1 → 100ms, attempt 2 → 200ms, attempt 3 → 400ms (`BACKOFF_TIME_MS << i`).
const BACKOFF_TIME_MS: u64 = 100;
/// Max RPC attempts for lock-contention / transient failures on get and prewrite.
const RETRY_TIMES: usize = 3;

/// Percolator transaction client.
///
/// Talks to the TSO for timestamps and to the transaction service for
/// snapshot reads and 2PC (`prewrite` → `commit`). Writes are buffered
/// locally until [`commit`](Self::commit). Supports multiple distinct keys
/// in one transaction; on abort, successful prewrites are rolled back.
#[derive(Clone)]
pub struct Client {
    tso_client: TSOClient,
    txn_client: TransactionClient,
    /// Snapshot timestamp for this txn; assigned in [`begin`](Self::begin).
    start_ts: u64,
    /// Pending writes (key, value), applied only at commit via prewrite.
    writes: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Client {
    /// Creates a client bound to the given TSO and transaction RPC stubs.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        Client {
            tso_client,
            txn_client,
            start_ts: 0,
            writes: Vec::new(),
        }
    }

    /// Fetches a strictly increasing timestamp from the Timestamp Oracle.
    pub fn get_timestamp(&self) -> Result<u64> {
        self.tso_client
            .get_timestamp(TimestampRequest {})
            .map(|resp| resp.timestamp)
    }

    /// Starts a transaction: take `start_ts` and clear the write buffer.
    pub fn begin(&mut self) {
        self.start_ts = self.get_timestamp().unwrap_or(0);
        self.writes.clear();
    }

    /// Snapshot get at `start_ts`.
    ///
    /// Prefers the local write buffer (read-your-writes). Otherwise calls the
    /// server `get` RPC, retrying with exponential backoff if the key is locked.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Read-your-writes: latest buffered value for this key wins.
        for (k, v) in self.writes.iter().rev() {
            if k == &key {
                return Ok(v.clone());
            }
        }

        for i in 0..RETRY_TIMES {
            match self.txn_client.get(GetRequest {
                key: key.clone(),
                start_ts: self.start_ts,
            }) {
                Ok(resp) => return Ok(resp.value),
                Err(e) if i + 1 < RETRY_TIMES => {
                    thread::sleep(Duration::from_millis(BACKOFF_TIME_MS << i));
                    let _ = e;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }

    /// Stages a write in the local buffer; nothing is sent until commit.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.writes.push((key, value));
    }

    /// Collapse duplicate keys so each key is prewritten once (last `set` wins).
    fn coalesced_writes(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut by_key: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut order: Vec<Vec<u8>> = Vec::new();
        for (key, value) in &self.writes {
            if !by_key.contains_key(key) {
                order.push(key.clone());
            }
            by_key.insert(key.clone(), value.clone());
        }
        order
            .into_iter()
            .map(|k| {
                let v = by_key.remove(&k).unwrap();
                (k, v)
            })
            .collect()
    }

    /// Best-effort unlock of keys that were prewritten but not committed.
    fn rollback_prewrites(&self, keys: &[Vec<u8>]) {
        for key in keys {
            let _ = self.txn_client.rollback(RollbackRequest {
                key: key.clone(),
                start_ts: self.start_ts,
            });
        }
    }

    /// Two-phase commit over all buffered writes (multi-key supported).
    ///
    /// 1. Coalesce writes (last value per key)
    /// 2. **Prewrite** each key; on failure, rollback earlier prewrites
    /// 3. Obtain `commit_ts` from TSO
    /// 4. **Commit** each key; on failure, rollback keys not yet committed
    ///
    /// Without primary/secondary locks, keys already committed in step 4 stay
    /// visible if a later key fails — documented limitation.
    ///
    /// Returns `Ok(false)` on conflict after retries; `Ok(true)` on success.
    pub fn commit(&self) -> Result<bool> {
        if self.writes.is_empty() {
            return Ok(true);
        }

        let writes = self.coalesced_writes();
        let mut prewritten: Vec<Vec<u8>> = Vec::new();

        // Phase 1: prewrite — lock keys and stage values.
        for (key, value) in &writes {
            let mut ok = false;
            for i in 0..RETRY_TIMES {
                let req = PrewriteRequest {
                    key: key.clone(),
                    value: value.clone(),
                    start_ts: self.start_ts,
                };
                match self.txn_client.prewrite(req) {
                    Ok(_) => {
                        ok = true;
                        break;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("write conflict") || i + 1 == RETRY_TIMES {
                            self.rollback_prewrites(&prewritten);
                            return Ok(false);
                        }
                        thread::sleep(Duration::from_millis(BACKOFF_TIME_MS << i));
                    }
                }
            }
            if !ok {
                self.rollback_prewrites(&prewritten);
                return Ok(false);
            }
            prewritten.push(key.clone());
        }

        let commit_ts = match self.get_timestamp() {
            Ok(ts) => ts,
            Err(e) => {
                self.rollback_prewrites(&prewritten);
                return Err(e);
            }
        };

        // Phase 2: commit — publish Write records and release locks.
        let mut committed = 0usize;
        for (key, value) in &writes {
            let req = CommitRequest {
                key: key.clone(),
                value: value.clone(),
                start_ts: self.start_ts,
                commit_ts,
            };
            match self.txn_client.commit(req) {
                Ok(resp) if resp.ok => {
                    committed += 1;
                }
                Ok(_) | Err(_) => {
                    // Roll back keys that still hold locks (not yet committed).
                    self.rollback_prewrites(&prewritten[committed..]);
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}
