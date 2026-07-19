use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::msg::*;
use crate::service::*;
use crate::*;

/// How long a lock may live before another txn can treat it as abandoned and clean it up.
/// Compared against timestamp differences (TSO values are nanosecond wall-clock based).
/// If the lock is still within TTL, callers should back off instead of cleaning it up.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

/// Timestamp Oracle (TSO): hands out strictly increasing timestamps used as
/// transaction start_ts / commit_ts for MVCC ordering.
#[derive(Clone, Default)]
pub struct TimestampOracle {
    last_ts: Arc<Mutex<u64>>,
}

impl TimestampOracle {
    pub fn new() -> Self {
        Self {
            last_ts: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    /// Returns a timestamp strictly greater than any previously issued value.
    /// Prefers wall-clock nanos; falls back to last_ts + 1 if the clock did not advance.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        let mut last_ts = self.last_ts.lock().unwrap();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_nanos() as u64;
        let timestamp = if now > *last_ts { now } else { *last_ts + 1 };
        *last_ts = timestamp;

        Ok(TimestampResponse { timestamp })
    }
}

/// Bigtable-style row key: (user key, timestamp).
pub type Key = (Vec<u8>, u64);

/// Cell value: either a timestamp pointer (Write/Lock columns) or raw bytes (Data column).
#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

/// Percolator stores each key across three columns:
/// - **Data**: the actual value written at `start_ts` during prewrite
/// - **Lock**: present while a txn has prewritten but not yet committed
/// - **Write**: commit record at `commit_ts`; value points back to Data's `start_ts`
pub enum Column {
    Write,
    Data,
    Lock,
}

/// In-memory stand-in for Google Bigtable used by Percolator's MVCC protocol.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    /// Latest record for `key` in `column` whose timestamp is in
    /// `[ts_start_inclusive, ts_end_inclusive]` (defaults: `0..=u64::MAX`).
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        let map = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };

        let ts_start = ts_start_inclusive.unwrap_or(0);
        let ts_end = ts_end_inclusive.unwrap_or(u64::MAX);

        // BTreeMap is ordered by (key, ts); scan the range newest-first.
        map.range((key.clone(), ts_start)..=(key.clone(), ts_end))
            .rev()
            .find(|((k, _), _)| k == &key)
            .map(|(k, v)| (k, v))
    }

    /// Insert / overwrite a cell at `(key, ts)` in the given column.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        let map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.insert((key, ts), value);
    }

    /// Remove the cell at `(key, commit_ts)` in the given column.
    #[inline]
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        let map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.remove(&(key, commit_ts));
    }
}

/// Thread-safe MVCC store implementing the transaction RPC service
/// (`get` / `prewrite` / `commit`).
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    /// Snapshot read at `req.start_ts`:
    /// 1. Clear a stale lock if possible; fail if the key is still locked
    /// 2. Find the latest Write with `commit_ts <= start_ts`
    /// 3. Follow that Write's pointer to the Data version and return its bytes
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Try to clear a stale lock first (no-op if unlocked / still fresh).
        self.back_off_maybe_clean_up_lock(req.start_ts, req.key.clone());

        let table = self.data.lock().unwrap();
        // Fresh lock still present — client should back off and retry.
        if table
            .read(req.key.clone(), Column::Lock, None, None)
            .is_some()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "key is locked",
            ));
        }

        // Snapshot read: latest Write with commit_ts <= start_ts, then follow to Data.
        match table.read(req.key.clone(), Column::Write, None, Some(req.start_ts)) {
            Some((_, Value::Timestamp(data_ts))) => {
                let data_ts = *data_ts;
                match table.read(req.key, Column::Data, Some(data_ts), Some(data_ts)) {
                    Some((_, Value::Vector(v))) => Ok(GetResponse { value: v.clone() }),
                    _ => Ok(GetResponse { value: vec![] }),
                }
            }
            _ => Ok(GetResponse { value: vec![] }),
        }
    }

    /// First phase of 2PC: lock the key and stage the new value at `start_ts`.
    /// Aborts on a live lock conflict or a write committed after `start_ts`.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        let mut table = self.data.lock().unwrap();

        // Another txn holds (or left) a lock on this key.
        if let Some(((_, lock_ts), _)) = table.read(req.key.clone(), Column::Lock, None, None) {
            let lock_ts_copy = *lock_ts;
            if req.start_ts.saturating_sub(lock_ts_copy) <= TTL {
                // Lock still within TTL — true conflict.
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "lock conflict",
                ));
            }
            // Stale lock from a crashed/aborted txn — remove Lock + staged Data.
            table.erase(req.key.clone(), Column::Data, lock_ts_copy);
            table.erase(req.key.clone(), Column::Lock, lock_ts_copy);
        }

        // A commit after our start_ts means we would overwrite a newer snapshot.
        if let Some(((_, write_ts), _)) =
            table.read(req.key.clone(), Column::Write, Some(req.start_ts + 1), None)
        {
            if *write_ts > req.start_ts {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "write conflict",
                ));
            }
        }

        // Stage: Lock marks the in-flight txn; Data holds the pending value.
        table.write(
            req.key.clone(),
            Column::Lock,
            req.start_ts,
            Value::Timestamp(req.start_ts),
        );
        table.write(
            req.key,
            Column::Data,
            req.start_ts,
            Value::Vector(req.value),
        );

        Ok(PrewriteResponse {})
    }

    /// Second phase of 2PC: publish the commit record and drop the lock.
    /// The Write cell at `commit_ts` stores `start_ts` so readers can find Data.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        let mut table = self.data.lock().unwrap();

        // Must still own the prewrite lock; otherwise commit is rejected.
        if table
            .read(
                req.key.clone(),
                Column::Lock,
                Some(req.start_ts),
                Some(req.start_ts),
            )
            .is_none()
        {
            return Ok(CommitResponse { ok: false });
        }

        // Make the version visible to future snapshot reads, then unlock.
        table.write(
            req.key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        table.erase(req.key, Column::Lock, req.start_ts);

        Ok(CommitResponse { ok: true })
    }

    /// Abort a staged prewrite: remove Lock and Data at `start_ts` if present.
    async fn rollback(&self, req: RollbackRequest) -> labrpc::Result<RollbackResponse> {
        let mut table = self.data.lock().unwrap();

        if table
            .read(
                req.key.clone(),
                Column::Lock,
                Some(req.start_ts),
                Some(req.start_ts),
            )
            .is_some()
        {
            table.erase(req.key.clone(), Column::Lock, req.start_ts);
            table.erase(req.key, Column::Data, req.start_ts);
        }

        Ok(RollbackResponse {})
    }
}

impl MemoryStorage {
    /// If `key` has a lock older than TTL relative to `start_ts`, erase that
    /// Lock and its uncommitted Data so readers/writers can proceed.
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        let mut table = self.data.lock().unwrap();

        if let Some(((_, lock_ts), Value::Timestamp(lock_start_ts))) =
            table.read(key.clone(), Column::Lock, None, None)
        {
            let lock_ts_copy = *lock_ts;
            let lock_start_ts_copy = *lock_start_ts;

            if start_ts.saturating_sub(lock_ts_copy) > TTL {
                table.erase(key.clone(), Column::Lock, lock_ts_copy);
                table.erase(key, Column::Data, lock_start_ts_copy);
            }
        }
    }
}
