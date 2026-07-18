use std::thread;
use std::time::Duration;

use labrpc::*;

use crate::msg::*;
use crate::service::{TSOClient, TransactionClient};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    tso_client: TSOClient,
    txn_client: TransactionClient,
    start_ts: u64,
    writes: Vec<(Vec<u8>, Vec<u8>)>, // Buffer for writes: (key, value) pairs
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        Client {
            tso_client,
            txn_client,
            start_ts: 0,
            writes: Vec::new(),
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        self.tso_client
            .get_timestamp(TimestampRequest {})
            .map(|resp| resp.timestamp)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        self.start_ts = self.get_timestamp().unwrap_or(0);
        self.writes.clear();
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // First check if the key is in the writes buffer (read-your-writes)
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

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.writes.push((key, value));
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        if self.writes.is_empty() {
            return Ok(true);
        }

        // Phase 1: Prewrite - lock and write data for all keys
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
                        if msg.contains("write conflict") {
                            return Ok(false);
                        }
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

        // Get commit timestamp from TSO
        let commit_ts = self.get_timestamp()?;

        // Phase 2: Commit - write commit record for all keys
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
