//! KV domain storage layer
//!
//! Implements key-value storage with route-based namespacing on top of KvStore trait.

use crate::storage::traits::KvStore;
use std::sync::Arc;

// Type alias for batch get result
type BatchGetResult = Vec<(String, Option<Vec<u8>>)>;

/// Wrap a KvStore and prepend route to key
pub struct KvStoreAdapter {
    kv: Arc<dyn KvStore>,
}

impl KvStoreAdapter {
    pub fn new(kv: Arc<dyn KvStore>) -> Self {
        Self { kv }
    }

    /// Build storage key: route as namespace + key
    fn build_key(route: &str, key: &str) -> Vec<u8> {
        let mut storage_key = Vec::new();
        storage_key.extend_from_slice(b"kv:");
        storage_key.extend_from_slice(route.as_bytes());
        storage_key.push(b'/');
        storage_key.extend_from_slice(key.as_bytes());
        storage_key
    }

    /// Put a key-value pair in a route namespace
    pub fn put(&self, route: &str, key: &str, value: Vec<u8>) -> Result<(), String> {
        let storage_key = Self::build_key(route, key);
        self.kv.put(&storage_key, &value)
    }

    /// Get a value by key
    pub fn get(&self, route: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
        let storage_key = Self::build_key(route, key);
        match self.kv.get(&storage_key)? {
            Some(bytes) => Ok(Some(bytes.to_vec())),
            None => Ok(None),
        }
    }

    /// Delete a key
    pub fn delete(&self, route: &str, key: &str) -> Result<(), String> {
        let storage_key = Self::build_key(route, key);
        self.kv.delete(&storage_key)
    }

    /// Scan keys >= start_key up to limit
    pub fn scan(
        &self,
        route: &str,
        start_key: &str,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, String> {
        let start_storage_key = Self::build_key(route, start_key);

        // Build end key: route namespace + max byte
        let mut end_storage_key = Vec::new();
        end_storage_key.extend_from_slice(b"kv:");
        end_storage_key.extend_from_slice(route.as_bytes());
        end_storage_key.push(b'/');
        end_storage_key.push(0xFF); // Max byte to scan entire route namespace

        let results = self.kv.scan(&start_storage_key, &end_storage_key)?;

        // Extract keys and values, removing the route prefix
        let prefix_len = format!("kv:{}/", route).len();
        let mut output = Vec::new();

        for (k, v) in results {
            if let Ok(key_str) = String::from_utf8(k[prefix_len..].to_vec()) {
                output.push((key_str, v.to_vec()));
                if output.len() >= limit {
                    break;
                }
            }
        }

        Ok(output)
    }

    /// Put multiple key-value pairs in a batch
    pub fn put_batch(&self, route: &str, items: Vec<(String, Vec<u8>)>) -> Result<(), String> {
        let writes: Vec<(Vec<u8>, Vec<u8>)> = items
            .into_iter()
            .map(|(k, v)| (Self::build_key(route, &k), v))
            .collect();

        self.kv.put_batch(writes)
    }

    /// Get multiple values by keys in a batch
    pub fn get_batch(
        &self,
        route: &str,
        keys: Vec<String>,
    ) -> Result<BatchGetResult, String> {
        let mut results = Vec::new();

        for key in keys {
            let value = self.get(route, &key)?;
            results.push((key, value));
        }

        Ok(results)
    }

    /// Delete all keys in range [start_key, end_key)
    pub fn delete_range(&self, route: &str, start_key: &str, end_key: &str) -> Result<u64, String> {
        let start_storage_key = Self::build_key(route, start_key);
        let end_storage_key = Self::build_key(route, end_key);

        // Get all keys in range
        let results = self.kv.scan(&start_storage_key, &end_storage_key)?;

        let count = results.len() as u64;

        // Delete them
        let keys_to_delete: Vec<Vec<u8>> = results.into_iter().map(|(k, _)| k.to_vec()).collect();
        if !keys_to_delete.is_empty() {
            self.kv.delete_batch(keys_to_delete)?;
        }

        Ok(count)
    }
}
