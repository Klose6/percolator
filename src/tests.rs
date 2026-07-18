#[cfg(test)]
mod integration_tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use crate::client::Client;
    use crate::server::{MemoryStorage, TimestampOracle};
    use crate::service::{TSOClient, TransactionClient};

    fn make_client(tso: TimestampOracle, storage: MemoryStorage) -> Client {
        let tso_client = TSOClient::with_service(tso);
        let txn_client = TransactionClient::with_service(storage);
        Client::new(tso_client, txn_client)
    }

    #[test]
    fn test_basic_transaction() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();
        let mut client = make_client(tso, storage);

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

        assert!(
            ts2 > ts1,
            "timestamps should be strictly increasing: {ts1} then {ts2}"
        );
    }

    #[test]
    fn test_empty_transaction() {
        let mut client = make_client(TimestampOracle::new(), MemoryStorage::default());

        client.begin();
        let result = client.commit().unwrap();
        assert!(result, "empty transaction should succeed");
    }

    #[test]
    fn test_overwrite_value() {
        let mut client = make_client(TimestampOracle::new(), MemoryStorage::default());

        client.begin();
        client.set(b"key".to_vec(), b"value1".to_vec());
        let val = client.get(b"key".to_vec()).unwrap();
        assert_eq!(val, b"value1");

        client.set(b"key".to_vec(), b"value2".to_vec());
        let val = client.get(b"key".to_vec()).unwrap();
        assert_eq!(val, b"value2");
    }

    #[test]
    fn test_committed_read_from_storage() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();

        let mut writer = make_client(tso.clone(), storage.clone());
        writer.begin();
        writer.set(b"k".to_vec(), b"v1".to_vec());
        assert!(writer.commit().unwrap());

        let mut reader = make_client(tso, storage);
        reader.begin();
        let val = reader.get(b"k".to_vec()).unwrap();
        assert_eq!(val, b"v1");
    }

    #[test]
    fn test_concurrent_commits_diff_keys() {
        let results = Arc::new(Mutex::new(Vec::new()));
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();
        let mut handles = vec![];
        let num_transactions = 10;

        for i in 0..num_transactions {
            let results_clone = Arc::clone(&results);
            let tso = tso.clone();
            let storage = storage.clone();

            let handle = thread::spawn(move || {
                let mut client = make_client(tso, storage);

                client.begin();
                let key = format!("key{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                client.set(key.clone(), value.clone());

                let val = client.get(key).unwrap();
                assert_eq!(val, value, "Transaction {} should read its own write", i);

                let commit_result = client.commit().unwrap();
                results_clone.lock().unwrap().push((i, commit_result));
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let results = results.lock().unwrap();
        assert_eq!(
            results.len(),
            num_transactions,
            "All {num_transactions} transactions should complete"
        );

        for (_, commit_result) in results.iter() {
            assert!(*commit_result, "All commits should succeed");
        }
    }

    #[test]
    fn test_concurrent_commits_same_keys() {
        let results = Arc::new(Mutex::new(Vec::new()));
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();
        let mut handles = vec![];
        let num_transactions = 10;
        let key = b"test_key".to_vec();

        for i in 0..num_transactions {
            let results_clone = Arc::clone(&results);
            let tso = tso.clone();
            let storage = storage.clone();
            let key = key.clone();

            let handle = thread::spawn(move || {
                let mut client = make_client(tso, storage);

                client.begin();
                let value = format!("value{}", i).into_bytes();
                client.set(key.clone(), value.clone());

                let val = client.get(key).unwrap();
                assert_eq!(
                    val, value,
                    "Transaction {} should read its own write",
                    i
                );

                let commit_result = client.commit().unwrap();
                results_clone
                    .lock()
                    .unwrap()
                    .push((i, commit_result, value));
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let results = results.lock().unwrap();
        assert_eq!(
            results.len(),
            num_transactions,
            "All {num_transactions} transactions should complete"
        );

        let successes: Vec<_> = results
            .iter()
            .filter(|(_, ok, _)| *ok)
            .map(|(i, _, v)| (*i, v.clone()))
            .collect();
        assert!(
            !successes.is_empty(),
            "at least one same-key writer should commit"
        );

        let mut reader = make_client(tso, storage);
        reader.begin();
        let final_val = reader.get(key).unwrap();
        let committed_values: HashSet<_> = successes.into_iter().map(|(_, v)| v).collect();
        assert!(
            committed_values.contains(&final_val),
            "final read {:?} must be one of the committed values",
            String::from_utf8_lossy(&final_val)
        );
    }

    #[test]
    fn test_parallel_reads_writes_same_key() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();
        let key = b"shared".to_vec();
        let seed = b"v0".to_vec();

        // Seed a committed value.
        {
            let mut seeder = make_client(tso.clone(), storage.clone());
            seeder.begin();
            seeder.set(key.clone(), seed.clone());
            assert!(seeder.commit().unwrap(), "seed commit should succeed");
        }

        let committed = Arc::new(Mutex::new(HashSet::from([seed.clone()])));
        let read_values = Arc::new(Mutex::new(Vec::new()));
        let mut handles = vec![];

        let num_writers = 8;
        let num_readers = 8;

        for i in 0..num_writers {
            let tso = tso.clone();
            let storage = storage.clone();
            let key = key.clone();
            let committed = Arc::clone(&committed);

            handles.push(thread::spawn(move || {
                let mut client = make_client(tso, storage);
                client.begin();
                let value = format!("vw{}", i).into_bytes();
                client.set(key, value.clone());
                if client.commit().unwrap() {
                    committed.lock().unwrap().insert(value);
                }
            }));
        }

        for _ in 0..num_readers {
            let tso = tso.clone();
            let storage = storage.clone();
            let key = key.clone();
            let read_values = Arc::clone(&read_values);

            handles.push(thread::spawn(move || {
                let mut client = make_client(tso, storage);
                client.begin();
                // Retry get briefly if the key is locked by a concurrent writer.
                let val = client.get(key).expect("reader get should not fail permanently");
                read_values.lock().unwrap().push(val);
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let committed = committed.lock().unwrap();
        let read_values = read_values.lock().unwrap();
        assert_eq!(read_values.len(), num_readers);

        for val in read_values.iter() {
            assert!(
                committed.contains(val),
                "reader saw {:?}, which was never successfully committed",
                String::from_utf8_lossy(val)
            );
        }

        let mut final_reader = make_client(tso, storage);
        final_reader.begin();
        let final_val = final_reader.get(key).unwrap();
        assert!(
            committed.contains(&final_val),
            "final value {:?} must be a committed value",
            String::from_utf8_lossy(&final_val)
        );
    }
}
