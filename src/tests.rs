#[cfg(test)]
mod integration_tests {
    use crate::server::{TimestampOracle, MemoryStorage};
    use crate::client::Client;
    use crate::service::{TSOClient, TransactionClient};

    #[test]
    fn test_basic_transaction() {
        // Create server instances
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();
        
        // Create client
        let tso_client = TSOClient::new();
        let txn_client = TransactionClient::new();
        let mut client = Client::new(tso_client, txn_client);
        
        // Begin a transaction
        client.begin();
        
        // Set some values
        client.set(b"key1".to_vec(), b"value1".to_vec());
        client.set(b"key2".to_vec(), b"value2".to_vec());
        
        // Get values (should read from write buffer)
        let val1 = client.get(b"key1".to_vec()).unwrap();
        assert_eq!(val1, b"value1");
        
        let val2 = client.get(b"key2".to_vec()).unwrap();
        assert_eq!(val2, b"value2");
        
        // Commit transaction
        let result = client.commit().unwrap();
        assert!(result, "commit should succeed");
    }

    #[test]
    fn test_get_timestamp() {
        let tso_client = TSOClient::new();
        let txn_client = TransactionClient::new();
        let client = Client::new(tso_client, txn_client);
        
        // Get timestamps - should return a Result
        let ts1 = client.get_timestamp();
        let ts2 = client.get_timestamp();
        
        // Both should be Ok
        assert!(ts1.is_ok());
        assert!(ts2.is_ok());

        // Timestamps should be sequential
        assert_eq!(ts1.unwrap() + 1, ts2.unwrap(), "timestamps should be sequential");
    }

    #[test]
    fn test_empty_transaction() {
        let tso_client = TSOClient::new();
        let txn_client = TransactionClient::new();
        let mut client = Client::new(tso_client, txn_client);
        
        client.begin();
        
        // Commit without writing anything
        let result = client.commit().unwrap();
        assert!(result, "empty transaction should succeed");
    }

    #[test]
    fn test_overwrite_value() {
        let tso_client = TSOClient::new();
        let txn_client = TransactionClient::new();
        let mut client = Client::new(tso_client, txn_client);
        
        client.begin();
        
        // Set a value
        client.set(b"key".to_vec(), b"value1".to_vec());
        let val = client.get(b"key".to_vec()).unwrap();
        assert_eq!(val, b"value1");
        
        // Overwrite it
        client.set(b"key".to_vec(), b"value2".to_vec());
        let val = client.get(b"key".to_vec()).unwrap();
        // Should get the latest value
        assert_eq!(val, b"value2");
    }

    #[test]
    fn test_concurrent_commits() {
        use std::sync::Arc;
        use std::sync::Mutex;
        use std::thread;

        let results = Arc::new(Mutex::new(Vec::new()));
        let mut handles = vec![];

        // Spawn 5 concurrent transactions
        for i in 0..5 {
            let results_clone = Arc::clone(&results);
            
            let handle = thread::spawn(move || {
                let tso_client = TSOClient::new();
                let txn_client = TransactionClient::new();
                let mut client = Client::new(tso_client, txn_client);
                
                // Each transaction commits different keys
                client.begin();
                
                let key = format!("key{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                
                client.set(key.clone(), value.clone());
                
                // Verify we can read our own write
                let val = client.get(key).unwrap();
                assert_eq!(val, value, "Transaction {} should read its own write", i);
                
                // Commit the transaction
                let commit_result = client.commit().unwrap();
                
                // Store the result
                let mut res = results_clone.lock().unwrap();
                res.push((i, commit_result));
            });
            
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify all commits succeeded
        let results = results.lock().unwrap();
        assert_eq!(results.len(), 5, "All 5 transactions should complete");
        
        for (_, commit_result) in results.iter() {
            assert!(*commit_result, "All commits should succeed");
        }
    }
}
