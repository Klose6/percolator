use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

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
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        let mut last_ts = self.last_ts.lock().unwrap();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_nanos() as u64;
        // Ensure that the returned timestamp is always greater than the last one.
        let timestamp = if now > *last_ts { now } else { *last_ts + 1 };
        *last_ts = timestamp;

        Ok(TimestampResponse { timestamp })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
         // Select the appropriate column
        let map = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };

        // Define the timestamp range
        let ts_start = ts_start_inclusive.unwrap_or(0);
        let ts_end = ts_end_inclusive.unwrap_or(u64::MAX);

        // Find the latest entry within the timestamp range
        // We iterate in reverse to find the most recent timestamp first
        map.range((key.clone(), ts_start)..=(key.clone(), ts_end))
            .rev()
            .find(|((k, _), _)| k == &key)
            .map(|(k, v)| (k, v))
    }

    // Writes a record to a specified column in MemoryStorage.
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
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
       let map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.remove(&(key, commit_ts));
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
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

    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        let mut table = self.data.lock().unwrap();

        // Check for lock conflict - if any lock exists on this key, abort
        if let Some(((_, lock_ts), _)) = table.read(req.key.clone(), Column::Lock, None, None) {
            let lock_ts_copy = *lock_ts;
            // Check if the lock is stale (older than TTL)
            if req.start_ts.saturating_sub(lock_ts_copy) <= TTL {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "lock conflict",
                ));
            }
            // Lock is stale, clean it up and continue
            table.erase(req.key.clone(), Column::Data, lock_ts_copy);
            table.erase(req.key.clone(), Column::Lock, lock_ts_copy);
        }

        // Check for write conflict - if there's a write at timestamp > start_ts, abort
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

        // No conflicts, write lock and data at start_ts
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

    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        let mut table = self.data.lock().unwrap();

        // Require the lock placed during prewrite.
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

        // Write column at commit_ts points to the Data version at start_ts.
        table.write(
            req.key.clone(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        table.erase(req.key, Column::Lock, req.start_ts);

        Ok(CommitResponse { ok: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        let mut table = self.data.lock().unwrap();
        
        // Try to find the lock on this key
        if let Some(((_, lock_ts), Value::Timestamp(lock_start_ts))) = 
            table.read(key.clone(), Column::Lock, None, None) {
            
            let lock_ts_copy = *lock_ts;
            let lock_start_ts_copy = *lock_start_ts;
            
            // Check if lock is stale (older than TTL)
            if start_ts.saturating_sub(lock_ts_copy) > TTL {
                // Lock is expired, clean it up
                table.erase(key.clone(), Column::Lock, lock_ts_copy);
                
                // Also clean up the associated data that was written but not committed
                table.erase(key, Column::Data, lock_start_ts_copy);
            }
        }
    }
}