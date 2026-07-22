use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use labrpc::*;

use crate::msg::*;
use crate::service::{TSOClient, TransactionClient};

/// Base wait (ms) before retrying a failed RPC. Multiplied exponentially per attempt.
const BACKOFF_TIME_MS: u64 = 100;
/// Max RPC attempts for lock-contention / transient failures.
const RETRY_TIMES: usize = 3;

/// Transaction locking mode (TiKV-style).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TxnMode {
    /// Locks acquired only at prewrite (classic Percolator). Default.
    #[default]
    Optimistic,
    /// Locks acquired on `set` / `lock_for_update` during execution.
    Pessimistic,
}

/// Percolator transaction client (optimistic and pessimistic).
#[derive(Clone)]
pub struct Client {
    tso_client: TSOClient,
    txn_client: TransactionClient,
    mode: TxnMode,
    /// Snapshot timestamp for this txn; assigned in [`begin`](Self::begin).
    start_ts: u64,
    /// Pending writes (key, value), applied at commit via prewrite.
    writes: Vec<(Vec<u8>, Vec<u8>)>,
    /// Keys locked early in pessimistic mode (may include keys not yet in `writes`).
    locked_keys: Vec<Vec<u8>>,
    /// Primary key for this txn (first locked or written key).
    primary: Option<Vec<u8>>,
}

impl Client {
    /// Creates an **optimistic** client (locks at prewrite only).
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        Self::with_mode(tso_client, txn_client, TxnMode::Optimistic)
    }

    /// Creates a client with an explicit locking mode.
    pub fn with_mode(
        tso_client: TSOClient,
        txn_client: TransactionClient,
        mode: TxnMode,
    ) -> Client {
        Client {
            tso_client,
            txn_client,
            mode,
            start_ts: 0,
            writes: Vec::new(),
            locked_keys: Vec::new(),
            primary: None,
        }
    }

    pub fn mode(&self) -> TxnMode {
        self.mode
    }

    /// Fetches a strictly increasing timestamp from the Timestamp Oracle.
    pub fn get_timestamp(&self) -> Result<u64> {
        self.tso_client
            .get_timestamp(TimestampRequest {})
            .map(|resp| resp.timestamp)
    }

    /// Starts a transaction: take `start_ts` and clear buffers / locks state.
    pub fn begin(&mut self) {
        self.start_ts = self.get_timestamp().unwrap_or(0);
        self.writes.clear();
        self.locked_keys.clear();
        self.primary = None;
    }

    /// Snapshot get at `start_ts`.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
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

    /// Buffer a write. In pessimistic mode, also acquires a lock immediately.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if self.mode == TxnMode::Pessimistic {
            self.lock_for_update(key.clone())?;
        }
        self.writes.push((key, value));
        Ok(())
    }

    /// Pessimistic `SELECT ... FOR UPDATE`: lock `key` now without writing a value.
    pub fn lock_for_update(&mut self, key: Vec<u8>) -> Result<()> {
        if self.mode != TxnMode::Pessimistic {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "lock_for_update requires pessimistic mode",
            ));
        }
        if self.locked_keys.iter().any(|k| k == &key) {
            return Ok(());
        }

        let primary = self.primary.clone().unwrap_or_else(|| key.clone());
        if self.primary.is_none() {
            self.primary = Some(key.clone());
        }

        let for_update_ts = self.get_timestamp()?;
        for i in 0..RETRY_TIMES {
            let req = PessimisticLockRequest {
                key: key.clone(),
                start_ts: self.start_ts,
                for_update_ts,
                primary: primary.clone(),
            };
            match self.txn_client.pessimistic_lock(req) {
                Ok(_) => {
                    self.locked_keys.push(key);
                    return Ok(());
                }
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("write conflict") || i + 1 == RETRY_TIMES {
                        return Err(e);
                    }
                    thread::sleep(Duration::from_millis(BACKOFF_TIME_MS << i));
                }
            }
        }
        unreachable!()
    }

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

    fn txn_primary(&self, writes: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
        self.primary
            .clone()
            .or_else(|| writes.first().map(|(k, _)| k.clone()))
            .unwrap_or_default()
    }

    fn rollback_keys(&self, keys: &[Vec<u8>]) {
        for key in keys {
            let _ = self.txn_client.rollback(RollbackRequest {
                key: key.clone(),
                start_ts: self.start_ts,
            });
        }
    }

    /// Two-phase commit. Optimistic: lock at prewrite. Pessimistic: downgrade
    /// existing locks and write Data at prewrite, then commit.
    pub fn commit(&self) -> Result<bool> {
        if self.writes.is_empty() {
            // Pessimistic locks without writes: just release them.
            if !self.locked_keys.is_empty() {
                self.rollback_keys(&self.locked_keys);
            }
            return Ok(true);
        }

        let writes = self.coalesced_writes();
        let primary = self.txn_primary(&writes);
        let mut prewritten: Vec<Vec<u8>> = Vec::new();

        for (key, value) in &writes {
            let mut ok = false;
            for i in 0..RETRY_TIMES {
                let req = PrewriteRequest {
                    key: key.clone(),
                    value: value.clone(),
                    start_ts: self.start_ts,
                    primary: primary.clone(),
                };
                match self.txn_client.prewrite(req) {
                    Ok(_) => {
                        ok = true;
                        break;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("write conflict") || i + 1 == RETRY_TIMES {
                            self.rollback_keys(&prewritten);
                            // Also release pessimistic locks not yet prewritten.
                            let extra: Vec<_> = self
                                .locked_keys
                                .iter()
                                .filter(|k| !prewritten.contains(k))
                                .cloned()
                                .collect();
                            self.rollback_keys(&extra);
                            return Ok(false);
                        }
                        thread::sleep(Duration::from_millis(BACKOFF_TIME_MS << i));
                    }
                }
            }
            if !ok {
                self.rollback_keys(&prewritten);
                return Ok(false);
            }
            prewritten.push(key.clone());
        }

        let commit_ts = match self.get_timestamp() {
            Ok(ts) => ts,
            Err(e) => {
                self.rollback_keys(&prewritten);
                return Err(e);
            }
        };

        // Commit primary first when it appears in the write set.
        let mut ordered = writes.clone();
        if let Some(pos) = ordered.iter().position(|(k, _)| k == &primary) {
            ordered.swap(0, pos);
        }

        let mut remaining: Vec<Vec<u8>> =
            ordered.iter().map(|(k, _)| k.clone()).collect();
        for (key, value) in &ordered {
            let req = CommitRequest {
                key: key.clone(),
                value: value.clone(),
                start_ts: self.start_ts,
                commit_ts,
            };
            match self.txn_client.commit(req) {
                Ok(resp) if resp.ok => {
                    remaining.retain(|k| k != key);
                }
                Ok(_) | Err(_) => {
                    self.rollback_keys(&remaining);
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}
