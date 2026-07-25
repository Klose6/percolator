#[cfg(test)]
mod integration_tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use crate::client::{Client, TxnMode};
    use crate::msg::PrewriteRequest;
    use crate::server::{MemoryStorage, TimestampOracle};
    use crate::service::{TSOClient, TransactionClient};

    fn make_client(tso: TimestampOracle, storage: MemoryStorage) -> Client {
        let tso_client = TSOClient::with_service(tso);
        let txn_client = TransactionClient::with_service(storage);
        Client::new(tso_client, txn_client)
    }

    fn make_pessimistic_client(tso: TimestampOracle, storage: MemoryStorage) -> Client {
        let tso_client = TSOClient::with_service(tso);
        let txn_client = TransactionClient::with_service(storage);
        Client::with_mode(tso_client, txn_client, TxnMode::Pessimistic)
    }

    /// Place a live lock on `key` so another txn's prewrite on that key fails.
    fn block_key(tso: &TimestampOracle, storage: &MemoryStorage, key: &[u8]) {
        let blocker = TransactionClient::with_service(storage.clone());
        let block_ts = make_client(tso.clone(), storage.clone())
            .get_timestamp()
            .unwrap();
        blocker
            .prewrite(PrewriteRequest {
                key: key.to_vec(),
                value: b"blocked".to_vec(),
                start_ts: block_ts,
                primary: key.to_vec(),
            })
            .expect("blocker prewrite should succeed");
    }

    // --- Single-key / basic tests ---

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
        client.set(b"key".to_vec(), b"value1".to_vec()).unwrap();
        let val = client.get(b"key".to_vec()).unwrap();
        assert_eq!(val, b"value1");

        client.set(b"key".to_vec(), b"value2".to_vec()).unwrap();
        let val = client.get(b"key".to_vec()).unwrap();
        assert_eq!(val, b"value2");
    }

    #[test]
    fn test_committed_read_from_storage() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();

        let mut writer = make_client(tso.clone(), storage.clone());
        writer.begin();
        writer.set(b"k".to_vec(), b"v1".to_vec()).unwrap();
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
                client.set(key.clone(), value.clone()).unwrap();

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
                client.set(key.clone(), value.clone()).unwrap();

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

        {
            let mut seeder = make_client(tso.clone(), storage.clone());
            seeder.begin();
            seeder.set(key.clone(), seed.clone()).unwrap();
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
                client.set(key, value.clone()).unwrap();
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

    // --- Multi-key transaction tests ---

    #[test]
    fn test_basic_multi_key_transaction() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();
        let mut client = make_client(tso.clone(), storage.clone());

        client.begin();
        client.set(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        client.set(b"key2".to_vec(), b"value2".to_vec()).unwrap();

        assert_eq!(client.get(b"key1".to_vec()).unwrap(), b"value1");
        assert_eq!(client.get(b"key2".to_vec()).unwrap(), b"value2");
        assert!(client.commit().unwrap());

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
        writer.set(b"a".to_vec(), b"va".to_vec()).unwrap();
        writer.set(b"b".to_vec(), b"vb".to_vec()).unwrap();
        writer.set(b"c".to_vec(), b"vc".to_vec()).unwrap();
        // Duplicate set: last value for `a` should win after coalesce.
        writer.set(b"a".to_vec(), b"va2".to_vec()).unwrap();
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

        block_key(&tso, &storage, b"key2");

        let mut client = make_client(tso.clone(), storage.clone());
        client.begin();
        client.set(b"key1".to_vec(), b"v1".to_vec()).unwrap();
        client.set(b"key2".to_vec(), b"v2".to_vec()).unwrap();
        assert!(
            !client.commit().unwrap(),
            "commit should fail due to lock on key2"
        );

        let mut writer = make_client(tso.clone(), storage.clone());
        writer.begin();
        writer.set(b"key1".to_vec(), b"ok".to_vec()).unwrap();
        assert!(
            writer.commit().unwrap(),
            "key1 should be free after rollback"
        );

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

        let start_ts = make_client(tso.clone(), storage.clone())
            .get_timestamp()
            .unwrap();
        let primary = b"pk".to_vec();
        let secondary = b"sk".to_vec();

        txn.prewrite(PrewriteRequest {
            key: primary.clone(),
            value: b"pv".to_vec(),
            start_ts,
            primary: primary.clone(),
        })
        .unwrap();
        txn.prewrite(PrewriteRequest {
            key: secondary.clone(),
            value: b"sv".to_vec(),
            start_ts,
            primary: primary.clone(),
        })
        .unwrap();

        let commit_ts = make_client(tso.clone(), storage.clone())
            .get_timestamp()
            .unwrap();
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

        let start_ts = make_client(tso.clone(), storage.clone())
            .get_timestamp()
            .unwrap();
        let primary = b"pk".to_vec();
        let secondary = b"sk".to_vec();

        txn.prewrite(PrewriteRequest {
            key: primary.clone(),
            value: b"pv".to_vec(),
            start_ts,
            primary: primary.clone(),
        })
        .unwrap();
        txn.prewrite(PrewriteRequest {
            key: secondary.clone(),
            value: b"sv".to_vec(),
            start_ts,
            primary: primary.clone(),
        })
        .unwrap();

        txn.rollback(crate::msg::RollbackRequest {
            key: primary,
            start_ts,
        })
        .unwrap();

        let mut reader = make_client(tso.clone(), storage.clone());
        reader.begin();
        assert!(
            reader.get(secondary.clone()).unwrap().is_empty(),
            "secondary must not be visible after primary rollback"
        );

        let mut writer = make_client(tso, storage);
        writer.begin();
        writer.set(secondary, b"ok".to_vec()).unwrap();
        assert!(writer.commit().unwrap());
    }

    /// Three-key transaction scenarios:
    /// 1) all three keys commit successfully
    /// 2) primary ok, 2nd key fails → full rollback
    /// 3) primary + 2nd ok, 3rd fails → full rollback
    #[test]
    fn test_three_key_write_success_and_rollback() {
        let k1 = b"k1".to_vec();
        let k2 = b"k2".to_vec();
        let k3 = b"k3".to_vec();

        // --- 1) Write all 3 keys in one transaction and succeed ---
        {
            let tso = TimestampOracle::new();
            let storage = MemoryStorage::default();
            let mut client = make_client(tso.clone(), storage.clone());
            client.begin();
            client.set(k1.clone(), b"v1".to_vec()).unwrap();
            client.set(k2.clone(), b"v2".to_vec()).unwrap();
            client.set(k3.clone(), b"v3".to_vec()).unwrap();
            assert!(client.commit().unwrap(), "all three keys should commit");

            let mut reader = make_client(tso, storage);
            reader.begin();
            assert_eq!(reader.get(k1.clone()).unwrap(), b"v1");
            assert_eq!(reader.get(k2.clone()).unwrap(), b"v2");
            assert_eq!(reader.get(k3.clone()).unwrap(), b"v3");
        }

        // --- 2) Primary prewrites, 2nd key errors → rollback (nothing committed) ---
        {
            let tso = TimestampOracle::new();
            let storage = MemoryStorage::default();
            block_key(&tso, &storage, &k2);

            let mut client = make_client(tso.clone(), storage.clone());
            client.begin();
            client.set(k1.clone(), b"a1".to_vec()).unwrap();
            client.set(k2.clone(), b"a2".to_vec()).unwrap();
            client.set(k3.clone(), b"a3".to_vec()).unwrap();
            assert!(
                !client.commit().unwrap(),
                "commit should fail when 2nd key is locked"
            );

            let mut reader = make_client(tso.clone(), storage.clone());
            reader.begin();
            assert!(reader.get(k1.clone()).unwrap().is_empty());
            assert!(reader.get(k3.clone()).unwrap().is_empty());

            // Primary must be unlocked after rollback.
            let mut writer = make_client(tso, storage);
            writer.begin();
            writer.set(k1.clone(), b"ok1".to_vec()).unwrap();
            assert!(writer.commit().unwrap());
        }

        // --- 3) Primary + 2nd prewrite, 3rd key errors → rollback ---
        {
            let tso = TimestampOracle::new();
            let storage = MemoryStorage::default();
            block_key(&tso, &storage, &k3);

            let mut client = make_client(tso.clone(), storage.clone());
            client.begin();
            client.set(k1.clone(), b"b1".to_vec()).unwrap();
            client.set(k2.clone(), b"b2".to_vec()).unwrap();
            client.set(k3.clone(), b"b3".to_vec()).unwrap();
            assert!(
                !client.commit().unwrap(),
                "commit should fail when 3rd key is locked"
            );

            let mut reader = make_client(tso.clone(), storage.clone());
            reader.begin();
            assert!(
                reader.get(k1.clone()).unwrap().is_empty(),
                "k1 must not stay committed after rollback"
            );
            assert!(
                reader.get(k2.clone()).unwrap().is_empty(),
                "k2 must not stay committed after rollback"
            );

            // Earlier keys must be free for a new writer.
            let mut writer = make_client(tso, storage);
            writer.begin();
            writer.set(k1, b"ok1".to_vec()).unwrap();
            writer.set(k2, b"ok2".to_vec()).unwrap();
            assert!(writer.commit().unwrap());
        }
    }

    // --- Optimistic vs pessimistic locking ---

    #[test]
    fn test_optimistic_locks_only_at_prewrite() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();

        let mut a = make_client(tso.clone(), storage.clone());
        a.begin();
        a.set(b"k".to_vec(), b"from-a".to_vec()).unwrap();
        // Not committed yet — another optimistic txn can still buffer a write locally.
        let mut b = make_client(tso.clone(), storage.clone());
        b.begin();
        b.set(b"k".to_vec(), b"from-b".to_vec()).unwrap();

        assert!(a.commit().unwrap());
        // b should lose at prewrite (write conflict or lock).
        assert!(!b.commit().unwrap());

        let mut reader = make_client(tso, storage);
        reader.begin();
        assert_eq!(reader.get(b"k".to_vec()).unwrap(), b"from-a");
    }

    #[test]
    fn test_pessimistic_locks_on_set() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();

        let mut holder = make_pessimistic_client(tso.clone(), storage.clone());
        holder.begin();
        holder.set(b"k".to_vec(), b"held".to_vec()).unwrap();
        // Lock is already on the server — second pessimistic txn cannot lock.
        let mut other = make_pessimistic_client(tso.clone(), storage.clone());
        other.begin();
        let err = other.set(b"k".to_vec(), b"other".to_vec());
        assert!(err.is_err(), "second pessimistic set should fail while key is locked");

        assert!(holder.commit().unwrap());

        let mut reader = make_client(tso, storage);
        reader.begin();
        assert_eq!(reader.get(b"k".to_vec()).unwrap(), b"held");
    }

    #[test]
    fn test_pessimistic_lock_for_update_then_set() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();

        let mut client = make_pessimistic_client(tso.clone(), storage.clone());
        client.begin();
        client.lock_for_update(b"k".to_vec()).unwrap();
        // Value written later; lock already held.
        client.set(b"k".to_vec(), b"v".to_vec()).unwrap();
        assert!(client.commit().unwrap());

        let mut reader = make_client(tso, storage);
        reader.begin();
        assert_eq!(reader.get(b"k".to_vec()).unwrap(), b"v");
    }

    #[test]
    fn test_pessimistic_multi_key_commit() {
        let tso = TimestampOracle::new();
        let storage = MemoryStorage::default();

        let mut client = make_pessimistic_client(tso.clone(), storage.clone());
        client.begin();
        client.set(b"a".to_vec(), b"1".to_vec()).unwrap();
        client.set(b"b".to_vec(), b"2".to_vec()).unwrap();
        assert!(client.commit().unwrap());

        let mut reader = make_client(tso, storage);
        reader.begin();
        assert_eq!(reader.get(b"a".to_vec()).unwrap(), b"1");
        assert_eq!(reader.get(b"b".to_vec()).unwrap(), b"2");
    }

    /// Pessimistic three-key scenarios (mirrors optimistic multi-key rollback tests):
    /// 1) all three keys commit successfully
    /// 2) primary locked, 2nd key fails → abort/rollback
    /// 3) primary + 2nd locked, 3rd fails → abort/rollback
    #[test]
    fn test_pessimistic_three_key_write_success_and_rollback() {
        let k1 = b"pk1".to_vec();
        let k2 = b"pk2".to_vec();
        let k3 = b"pk3".to_vec();

        // --- 1) Write all 3 keys in one pessimistic transaction and succeed ---
        {
            let tso = TimestampOracle::new();
            let storage = MemoryStorage::default();
            let mut client = make_pessimistic_client(tso.clone(), storage.clone());
            client.begin();
            client.set(k1.clone(), b"v1".to_vec()).unwrap();
            client.set(k2.clone(), b"v2".to_vec()).unwrap();
            client.set(k3.clone(), b"v3".to_vec()).unwrap();
            assert!(client.commit().unwrap(), "all three keys should commit");

            let mut reader = make_client(tso, storage);
            reader.begin();
            assert_eq!(reader.get(k1.clone()).unwrap(), b"v1");
            assert_eq!(reader.get(k2.clone()).unwrap(), b"v2");
            assert_eq!(reader.get(k3.clone()).unwrap(), b"v3");
        }

        // --- 2) Primary locks, 2nd key conflicts → auto-abort, nothing committed ---
        {
            let tso = TimestampOracle::new();
            let storage = MemoryStorage::default();
            block_key(&tso, &storage, &k2);

            let mut client = make_pessimistic_client(tso.clone(), storage.clone());
            client.begin();
            client.set(k1.clone(), b"a1".to_vec()).unwrap();
            assert!(
                client.set(k2.clone(), b"a2".to_vec()).is_err(),
                "2nd set should fail while key is locked"
            );
            // Failed set() aborts and releases the primary lock on k1.

            let mut writer = make_pessimistic_client(tso.clone(), storage.clone());
            writer.begin();
            writer.set(k1.clone(), b"ok1".to_vec()).unwrap();
            assert!(writer.commit().unwrap(), "k1 should be free after rollback");

            let mut reader = make_client(tso, storage);
            reader.begin();
            assert_eq!(reader.get(k1.clone()).unwrap(), b"ok1");
            assert!(reader.get(k2.clone()).unwrap().is_empty());
            assert!(reader.get(k3.clone()).unwrap().is_empty());
        }

        // --- 3) Primary + 2nd lock, 3rd conflicts → abort, nothing committed ---
        {
            let tso = TimestampOracle::new();
            let storage = MemoryStorage::default();
            block_key(&tso, &storage, &k3);

            let mut client = make_pessimistic_client(tso.clone(), storage.clone());
            client.begin();
            client.set(k1.clone(), b"b1".to_vec()).unwrap();
            client.set(k2.clone(), b"b2".to_vec()).unwrap();
            assert!(
                client.set(k3.clone(), b"b3".to_vec()).is_err(),
                "3rd set should fail while key is locked"
            );

            let mut reader = make_client(tso.clone(), storage.clone());
            reader.begin();
            assert!(
                reader.get(k1.clone()).unwrap().is_empty(),
                "k1 must not stay committed after abort"
            );
            assert!(
                reader.get(k2.clone()).unwrap().is_empty(),
                "k2 must not stay committed after abort"
            );

            // Earlier keys must be free for a new pessimistic writer.
            let mut writer = make_pessimistic_client(tso, storage);
            writer.begin();
            writer.set(k1, b"ok1".to_vec()).unwrap();
            writer.set(k2, b"ok2".to_vec()).unwrap();
            assert!(writer.commit().unwrap());
        }
    }
}
