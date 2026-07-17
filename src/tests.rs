#[cfg(test)]
mod integration_tests {
    use crate::server::{TimestampOracle, MemoryStorage};
    use crate::client::Client;
    use crate::service::{TSOClient, TransactionClient};

    #[test]
    fn test_basic_transaction() {
        let tso = TimestampOracle::new();
        let _storage = MemoryStorage::default();

        let tso_client = TSOClient::with_service(tso);
        let txn_client = TransactionClient::new();
        let mut client = Client::new(tso_client, txn_client);

        client.begin();

        client.set(b"key1".to_vec(), b"value1".to_vec());
        client.set(b"key2".to_vec(), b"value2".to_vec());

        let val1 = client.get(b"key1".to_vec()).unwrap();
        assert_eq!(val1, b"value1");

        let val2 = client.get(b"key2".to_vec()).unwrap();
        assert_eq!(val2, b"value2");

        let result = client.commit().unwrap();
        assert!(result, "commit should succeed");
    }

    #[test]
    fn test_get_timestamp() {
        let tso_client = TSOClient::with_service(TimestampOracle::new());
        let txn_client = TransactionClient::new();
        let client = Client::new(tso_client, txn_client);

        let ts1 = client.get_timestamp().unwrap();
        let ts2 = client.get_timestamp().unwrap();

        // TimestampOracle guarantees strictly increasing timestamps.
        assert!(ts2 > ts1, "timestamps should be strictly increasing: {ts1} then {ts2}");
    }

    #[test]
    fn test_empty_transaction() {
        let tso_client = TSOClient::with_service(TimestampOracle::new());
        let txn_client = TransactionClient::new();
        let mut client = Client::new(tso_client, txn_client);

        client.begin();

        let result = client.commit().unwrap();
        assert!(result, "empty transaction should succeed");
    }

    #[test]
    fn test_overwrite_value() {
        let tso_client = TSOClient::with_service(TimestampOracle::new());
        let txn_client = TransactionClient::new();
        let mut client = Client::new(tso_client, txn_client);

        client.begin();

        client.set(b"key".to_vec(), b"value1".to_vec());
        let val = client.get(b"key".to_vec()).unwrap();
        assert_eq!(val, b"value1");

        client.set(b"key".to_vec(), b"value2".to_vec());
        let val = client.get(b"key".to_vec()).unwrap();
        assert_eq!(val, b"value2");
    }

    #[test]
    fn test_concurrent_commits_diff_keys() {
        use std::sync::Arc;
        use std::sync::Mutex;
        use std::thread;

        let results = Arc::new(Mutex::new(Vec::new()));
        let tso = TimestampOracle::new();
        let mut handles = vec![];
        let num_transactions = 10;

        for i in 0..num_transactions {
            let results_clone = Arc::clone(&results);
            let tso = tso.clone();

            let handle = thread::spawn(move || {
                let tso_client = TSOClient::with_service(tso);
                let txn_client = TransactionClient::new();
                let mut client = Client::new(tso_client, txn_client);

                client.begin();

                let key = format!("key{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();

                client.set(key.clone(), value.clone());

                let val = client.get(key).unwrap();
                assert_eq!(val, value, "Transaction {} should read its own write", i);

                let commit_result = client.commit().unwrap();

                let mut res = results_clone.lock().unwrap();
                res.push((i, commit_result));
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let results = results.lock().unwrap();
        assert_eq!(results.len(), num_transactions, "All {num_transactions} transactions should complete");

        for (_, commit_result) in results.iter() {
            assert!(*commit_result, "All commits should succeed");
        }
    }

    #[test]
    fn test_concurrent_commits_same_keys() {
        use std::sync::Arc;
        use std::sync::Mutex;
        use std::thread;

        let results = Arc::new(Mutex::new(Vec::new()));
        let tso = TimestampOracle::new();
        let mut handles = vec![];
        let num_transactions = 10;
        let key = b"test_key".to_vec();

        for i in 0..num_transactions {
            let results_clone = Arc::clone(&results);
            let tso = tso.clone();
            let key = key.clone();

            let handle = thread::spawn( move || {
                let tso_client = TSOClient::with_service(tso);
                let txn_client = TransactionClient::new();
                let mut client = Client::new(tso_client, txn_client);

                client.begin();

                let value = format!("value{}", i).into_bytes();

                client.set(key.clone(), value.to_vec());

                let val = client.get(key.clone()).unwrap();
                assert_eq!(val, value.to_vec(), "Transaction {} should read its own write", i);

                let commit_result = client.commit().unwrap();

                let mut res = results_clone.lock().unwrap();
                res.push((i, commit_result));
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let results = results.lock().unwrap();
        assert_eq!(results.len(), num_transactions, "All {num_transactions} transactions should complete");

        for (_, commit_result) in results.iter() {
            assert!(*commit_result, "All commits should succeed");
        }
    }
}
