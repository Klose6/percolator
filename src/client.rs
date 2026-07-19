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
/// locally until [`commit`](Self::commit).
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

    /// Two-phase commit over all buffered writes.
    ///
    /// 1. **Prewrite** each key (lock + stage Data at `start_ts`)
    /// 2. Obtain `commit_ts` from TSO
    /// 3. **Commit** each key (Write record + drop Lock)
    ///
    /// Returns `Ok(false)` on write/lock conflict after retries; `Ok(true)` on success.
    pub fn commit(&self) -> Result<bool> {
        if self.writes.is_empty() {
            return Ok(true);
        }

        // Phase 1: prewrite — lock keys and stage values.
        for (key, value) in &self.writes {
            let mut prewrote = false;
            for i in 0..RETRY_TIMES {
                let req = PrewriteRequest {
                    key: key.clone(),
                    value: value.clone(),
                    start_ts: self.start_ts,
                };
                match self.txn_client.prewrite(req) {
                    Ok(_) => {
                        prewrote = true;
                        break;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        // Newer committed write — abort; retrying will not help.
                        if msg.contains("write conflict") {
                            return Ok(false);
                        }
                        // Likely lock conflict — back off and retry.
                        if i + 1 == RETRY_TIMES {
                            return Ok(false);
                        }
                        thread::sleep(Duration::from_millis(BACKOFF_TIME_MS << i));
                    }
                }
            }
            if !prewrote {
                return Ok(false);
            }
        }

        // Commit timestamp must be greater than start_ts (and all prior commits).
        let commit_ts = self.get_timestamp()?;

        // Phase 2: commit — publish Write records and release locks.
        for (key, value) in &self.writes {
            let req = CommitRequest {
                key: key.clone(),
                value: value.clone(),
                start_ts: self.start_ts,
                commit_ts,
            };
            let resp = self.txn_client.commit(req)?;
            if !resp.ok {
                return Ok(false);
            }
        }

        Ok(true)
    }
}
