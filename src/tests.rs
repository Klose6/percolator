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
        let mut client = make_client(tso.clone(), storage.clone());

        client.begin();
        client.set(b"key1".to_vec(), b"value1".to_vec());
        client.set(b"key2".to_vec(), b"value2".to_vec());

        let val1 = client.get(b"key1".to_vec()).unwrap();
        assert_eq!(val1, b"value1");

        let val2 = client.get(b"key2".to_vec()).unwrap();
        assert_eq!(val2, b"value2");

        let result = client.commit().unwrap();
        assert!(result, "commit should succeed");

        // Durable multi-key read from storage after commit.
        let mut reader = make_client(tso, storage);
        reader.begin();
        assert_eq!(reader.get(b"key1".to_vec()).unwrap(), b"value1");
        assert_eq!(reader.get(b"key2".to_vec()).unwrap(), b"value2");
    }

    #[test]
    fn test_multi_key_write_commit_and_read() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();

        let mut writer = make_client(tso.clone(), storage.clone());
        writer.begin();
        writer.set(b"a".to_vec(), b"va".to_vec());
        writer.set(b"b".to_vec(), b"vb".to_vec());
        writer.set(b"c".to_vec(), b"vc".to_vec());
        // Duplicate set: last value for `a` should win after coalesce.
        writer.set(b"a".to_vec(), b"va2".to_vec());
        assert!(writer.commit().unwrap());

        let mut reader = make_client(tso, storage);
        reader.begin();
        assert_eq!(reader.get(b"a".to_vec()).unwrap(), b"va2");
        assert_eq!(reader.get(b"b".to_vec()).unwrap(), b"vb");
        assert_eq!(reader.get(b"c".to_vec()).unwrap(), b"vc");
    }

    #[test]
    fn test_multi_key_prewrite_rollback() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();

        // Hold a live lock on key2 so a multi-key txn fails on the second prewrite.
        let blocker = TransactionClient::with_service(storage.clone());
        let block_ts = {
            let mut ts_client = make_client(tso.clone(), storage.clone());
            ts_client.get_timestamp().unwrap()
        };
        blocker
            .prewrite(crate::msg::PrewriteRequest {
                key: b"key2".to_vec(),
                value: b"blocked".to_vec(),
                start_ts: block_ts,
                primary: b"key2".to_vec(),
            })
            .expect("blocker prewrite should succeed");

        let mut client = make_client(tso.clone(), storage.clone());
        client.begin();
        client.set(b"key1".to_vec(), b"v1".to_vec());
        client.set(b"key2".to_vec(), b"v2".to_vec());
        assert!(
            !client.commit().unwrap(),
            "commit should fail due to lock on key2"
        );

        // key1 must not stay locked: a later writer should be able to commit it.
        let mut writer = make_client(tso.clone(), storage.clone());
        writer.begin();
        writer.set(b"key1".to_vec(), b"ok".to_vec());
        assert!(
            writer.commit().unwrap(),
            "key1 should be free after rollback"
        );

        // Failed txn must not have committed either key.
        let mut reader = make_client(tso, storage);
        reader.begin();
        assert_eq!(reader.get(b"key1".to_vec()).unwrap(), b"ok");
        assert!(
            reader.get(b"key2".to_vec()).unwrap().is_empty(),
            "key2 should have no committed value from the failed txn"
        );
    }

    /// Commit only the primary; a later get on the secondary should resolve via
    /// the primary Write and make the secondary value visible.
    #[test]
    fn test_secondary_resolves_after_primary_commit() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();
        let txn = TransactionClient::with_service(storage.clone());

        let start_ts = {
            let c = make_client(tso.clone(), storage.clone());
            c.get_timestamp().unwrap()
        };
        let primary = b"pk".to_vec();
        let secondary = b"sk".to_vec();

        txn.prewrite(crate::msg::PrewriteRequest {
            key: primary.clone(),
            value: b"pv".to_vec(),
            start_ts,
            primary: primary.clone(),
        })
        .unwrap();
        txn.prewrite(crate::msg::PrewriteRequest {
            key: secondary.clone(),
            value: b"sv".to_vec(),
            start_ts,
            primary: primary.clone(),
        })
        .unwrap();

        let commit_ts = {
            let c = make_client(tso.clone(), storage.clone());
            c.get_timestamp().unwrap()
        };
        // Commit primary only — leave secondary locked.
        let resp = txn
            .commit(crate::msg::CommitRequest {
                key: primary.clone(),
                value: b"pv".to_vec(),
                start_ts,
                commit_ts,
            })
            .unwrap();
        assert!(resp.ok);

        let mut reader = make_client(tso, storage);
        reader.begin();
        assert_eq!(
            reader.get(secondary).unwrap(),
            b"sv",
            "get should resolve secondary lock after primary commit"
        );
        assert_eq!(reader.get(primary).unwrap(), b"pv");
    }

    /// If the primary is rolled back, a get on the secondary should clear its
    /// lock and not expose uncommitted data.
    #[test]
    fn test_secondary_rolls_back_when_primary_aborts() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();
        let txn = TransactionClient::with_service(storage.clone());

        let start_ts = {
            let c = make_client(tso.clone(), storage.clone());
            c.get_timestamp().unwrap()
        };
        let primary = b"pk".to_vec();
        let secondary = b"sk".to_vec();

        txn.prewrite(crate::msg::PrewriteRequest {
            key: primary.clone(),
            value: b"pv".to_vec(),
            start_ts,
            primary: primary.clone(),
        })
        .unwrap();
        txn.prewrite(crate::msg::PrewriteRequest {
            key: secondary.clone(),
            value: b"sv".to_vec(),
            start_ts,
            primary: primary.clone(),
        })
        .unwrap();

        txn.rollback(crate::msg::RollbackRequest {
            key: primary.clone(),
            start_ts,
        })
        .unwrap();

        let mut reader = make_client(tso.clone(), storage.clone());
        reader.begin();
        assert!(
            reader.get(secondary.clone()).unwrap().is_empty(),
            "secondary must not be visible after primary rollback"
        );

        // Secondary lock should be gone so a new writer can commit.
        let mut writer = make_client(tso, storage);
        writer.begin();
        writer.set(secondary, b"ok".to_vec());
        assert!(writer.commit().unwrap());
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
    fn test_concurrent_one_key_write_diff_keys() {
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
    fn test_concurrent_one_key_write_same_key() {
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
    fn test_parallel_reads_one_key_writes_same_key() {
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
