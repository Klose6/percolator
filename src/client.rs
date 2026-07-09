use labrpc::*;

use crate::{server::TimestampOracle, service::{TSOClient, TransactionClient, timestamp, transaction}};
use crate::msg::*;

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
    writes: Vec<(Vec<u8>, Vec<u8>)>,  // Buffer for writes: (key, value) pairs
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
        timestamp::get_timestamp()
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        self.start_ts = self.get_timestamp().unwrap_or(0);
        self.writes.clear();
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // First check if the key is in the writes buffer (read-your-writes)
        // Return the LAST (most recent) value for this key
        for (k, v) in self.writes.iter().rev() {
            if k == &key {
                return Ok(v.clone());
            }
        }
        
        // TODO: Fetch from server using transaction service when RPC is fully implemented
        // For now, return empty value if not in writes buffer
        Ok(vec![])
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
            let req = PrewriteRequest {
                key: key.clone(),
                value: value.clone(),
                start_ts: self.start_ts,
            };
            // TODO: Call prewrite RPC when fully implemented
            // let resp = self.txn_client.prewrite(req)?;
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
            // TODO: Call commit RPC when fully implemented
            // let resp = self.txn_client.commit(req)?;
        }
        
        Ok(true)
    }
}