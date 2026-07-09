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
}
