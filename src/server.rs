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

/// Cell value: timestamp pointer (Write column), raw bytes (Data), or primary key (Lock).
#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

/// Percolator stores each key across three columns:
/// - **Data**: the actual value written at `start_ts` during prewrite
/// - **Lock**: present while a txn has prewritten but not yet committed;
///   value is the txn's **primary key** bytes (`Value::Vector`)
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

    /// Find the commit_ts of a Write on `key` whose value points at `data_ts` (start_ts).
    fn find_write_commit_ts(&self, key: &[u8], data_ts: u64) -> Option<u64> {
        self.write.iter().find_map(|((k, commit_ts), v)| {
            if k.as_slice() == key {
                if let Value::Timestamp(ts) = v {
                    if *ts == data_ts {
                        return Some(*commit_ts);
                    }
                }
            }
            None
        })
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

        // Stage: Lock stores the txn primary key; Data holds the pending value.
        let primary = if req.primary.is_empty() {
            req.key.clone()
        } else {
            req.primary.clone()
        };
        table.write(
            req.key.clone(),
            Column::Lock,
            req.start_ts,
            Value::Vector(primary),
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
    /// Resolve a lock on `key` using Percolator primary/secondary rules.
    ///
    /// - Primary: erase Lock+Data only if stale (TTL).
    /// - Secondary: if primary committed → commit this key; if primary gone →
    ///   rollback; if primary still locked and fresh → leave for caller to retry.
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        let mut table = self.data.lock().unwrap();

        let Some(((_, lock_ts), Value::Vector(primary))) =
            table.read(key.clone(), Column::Lock, None, None)
        else {
            return;
        };
        let lock_ts = *lock_ts;
        let primary = primary.clone();

        if primary == key {
            // This row is the primary lock.
            if start_ts.saturating_sub(lock_ts) > TTL {
                table.erase(key.clone(), Column::Lock, lock_ts);
                table.erase(key, Column::Data, lock_ts);
            }
            return;
        }

        // Secondary: follow the primary's outcome for this start_ts (`lock_ts`).
        if let Some(commit_ts) = table.find_write_commit_ts(&primary, lock_ts) {
            // Primary already committed — publish the same commit on this key.
            table.write(
                key.clone(),
                Column::Write,
                commit_ts,
                Value::Timestamp(lock_ts),
            );
            table.erase(key, Column::Lock, lock_ts);
            return;
        }

        let primary_still_locked = table
            .read(
                primary.clone(),
                Column::Lock,
                Some(lock_ts),
                Some(lock_ts),
            )
            .is_some();

        if primary_still_locked {
            if start_ts.saturating_sub(lock_ts) > TTL {
                // Stale primary — clean primary then roll back this secondary.
                table.erase(primary.clone(), Column::Lock, lock_ts);
                table.erase(primary, Column::Data, lock_ts);
                table.erase(key.clone(), Column::Lock, lock_ts);
                table.erase(key, Column::Data, lock_ts);
            }
            return;
        }

        // Primary has neither Lock nor matching Write → rolled back.
        table.erase(key.clone(), Column::Lock, lock_ts);
        table.erase(key, Column::Data, lock_ts);
    }
}
