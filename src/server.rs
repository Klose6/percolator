use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::msg::*;
use crate::service::*;
use crate::*;

/// How long a lock may live before another txn can treat it as abandoned and clean it up.
/// Compared against timestamp differences (TSO values are nanosecond wall-clock based).
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

/// Lock column encoding: `[kind:u8][primary...]` where kind 0 = prewrite, 1 = pessimistic.
fn encode_lock(primary: &[u8], pessimistic: bool) -> Value {
    let mut bytes = Vec::with_capacity(1 + primary.len());
    bytes.push(if pessimistic { 1 } else { 0 });
    bytes.extend_from_slice(primary);
    Value::Vector(bytes)
}

fn decode_lock(value: &Value) -> Option<(Vec<u8>, bool)> {
    match value {
        Value::Vector(bytes) if !bytes.is_empty() => {
            let pessimistic = bytes[0] == 1;
            Some((bytes[1..].to_vec(), pessimistic))
        }
        _ => None,
    }
}

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

/// Cell value: timestamp pointer (Write), raw bytes (Data), or encoded lock (Lock).
#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

/// Percolator columns:
/// - **Data**: value at `start_ts` (written at prewrite; not for pessimistic-only locks)
/// - **Lock**: in-flight txn; value encodes primary key + optimistic/pessimistic kind
/// - **Write**: commit record at `commit_ts` → points to Data `start_ts`
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

        map.range((key.clone(), ts_start)..=(key.clone(), ts_end))
            .rev()
            .find(|((k, _), _)| k == &key)
            .map(|(k, v)| (k, v))
    }

    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        let map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.insert((key, ts), value);
    }

    #[inline]
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        let map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.remove(&(key, commit_ts));
    }

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

/// Thread-safe MVCC store: optimistic 2PC + pessimistic locks (TiKV-style).
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    /// Snapshot read at `req.start_ts`.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        self.back_off_maybe_clean_up_lock(req.start_ts, req.key.clone());

        let table = self.data.lock().unwrap();
        // Fresh lock still present — in-flight txn may commit into this snapshot.
        if table
            .read(req.key.clone(), Column::Lock, None, None)
            .is_some()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "key is locked",
            ));
        }

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

    /// Lock a key during pessimistic txn execution (no Data yet).
    async fn pessimistic_lock(
        &self,
        req: PessimisticLockRequest,
    ) -> labrpc::Result<PessimisticLockResponse> {
        let mut table = self.data.lock().unwrap();
        let primary = if req.primary.is_empty() {
            req.key.clone()
        } else {
            req.primary.clone()
        };

        // Already hold our own lock at start_ts — idempotent success.
        if let Some(((_, lock_ts), value)) =
            table.read(req.key.clone(), Column::Lock, None, None)
        {
            let lock_ts = *lock_ts;
            if let Some((lock_primary, _)) = decode_lock(value) {
                if lock_ts == req.start_ts && lock_primary == primary {
                    return Ok(PessimisticLockResponse {});
                }
            }
            if req.for_update_ts.saturating_sub(lock_ts) <= TTL {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "lock conflict",
                ));
            }
            // Stale foreign lock — clean and continue.
            table.erase(req.key.clone(), Column::Data, lock_ts);
            table.erase(req.key.clone(), Column::Lock, lock_ts);
        }

        // Write committed after for_update_ts → conflict (TiKV pessimistic check).
        if let Some(((_, write_ts), _)) =
            table.read(req.key.clone(), Column::Write, Some(req.for_update_ts + 1), None)
        {
            if *write_ts > req.for_update_ts {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "write conflict",
                ));
            }
        }

        table.write(
            req.key,
            Column::Lock,
            req.start_ts,
            encode_lock(&primary, true),
        );
        Ok(PessimisticLockResponse {})
    }

    /// First phase of 2PC: lock + stage Data.
    /// If this txn already holds a pessimistic lock, downgrade it and write Data.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        let mut table = self.data.lock().unwrap();
        let primary = if req.primary.is_empty() {
            req.key.clone()
        } else {
            req.primary.clone()
        };

        if let Some(((_, lock_ts), value)) =
            table.read(req.key.clone(), Column::Lock, None, None)
        {
            let lock_ts = *lock_ts;
            if let Some((lock_primary, is_pessimistic)) = decode_lock(value) {
                // Our pessimistic lock → downgrade to prewrite lock and write Data.
                if lock_ts == req.start_ts && is_pessimistic && lock_primary == primary {
                    table.write(
                        req.key.clone(),
                        Column::Lock,
                        req.start_ts,
                        encode_lock(&primary, false),
                    );
                    table.write(
                        req.key,
                        Column::Data,
                        req.start_ts,
                        Value::Vector(req.value),
                    );
                    return Ok(PrewriteResponse {});
                }
            }
            if req.start_ts.saturating_sub(lock_ts) <= TTL {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "lock conflict",
                ));
            }
            table.erase(req.key.clone(), Column::Data, lock_ts);
            table.erase(req.key.clone(), Column::Lock, lock_ts);
        }

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

        table.write(
            req.key.clone(),
            Column::Lock,
            req.start_ts,
            encode_lock(&primary, false),
        );
        table.write(
            req.key,
            Column::Data,
            req.start_ts,
            Value::Vector(req.value),
        );

        Ok(PrewriteResponse {})
    }

    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        let mut table = self.data.lock().unwrap();

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

        table.write(
            req.key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        table.erase(req.key, Column::Lock, req.start_ts);

        Ok(CommitResponse { ok: true })
    }

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
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        let mut table = self.data.lock().unwrap();

        let Some(((_, lock_ts), value)) = table.read(key.clone(), Column::Lock, None, None) else {
            return;
        };
        let Some((primary, _)) = decode_lock(value) else {
            return;
        };
        let lock_ts = *lock_ts;

        if primary == key {
            if start_ts.saturating_sub(lock_ts) > TTL {
                table.erase(key.clone(), Column::Lock, lock_ts);
                table.erase(key, Column::Data, lock_ts);
            }
            return;
        }

        if let Some(commit_ts) = table.find_write_commit_ts(&primary, lock_ts) {
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
                table.erase(primary.clone(), Column::Lock, lock_ts);
                table.erase(primary, Column::Data, lock_ts);
                table.erase(key.clone(), Column::Lock, lock_ts);
                table.erase(key, Column::Data, lock_ts);
            }
            return;
        }

        table.erase(key.clone(), Column::Lock, lock_ts);
        table.erase(key, Column::Data, lock_ts);
    }
}
