//! KV domain service - simple key-value operations
//!
//! The KV service provides basic key-value operations with route-based key namespacing.
//! Keys are scoped by the route resource path for multi-tenancy.

use super::types::KvOperation;
use crate::storage::traits::KvStore;
use std::sync::Arc;

/// KV service handles key-value storage operations
/// - Put: store key-value pairs
/// - Get: retrieve values by key
/// - Delete: remove keys
/// - Scan: list keys with prefix
/// - Batch: atomic multi-operation transactions
/// - GetMany: retrieve multiple keys
/// - DeleteRange: remove keys in range
pub struct KvService {
    store: Arc<dyn KvStore>,
}

impl KvService {
    /// Create a new KV service
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self { store: kv_store }
    }

    /// Build namespaced key from route and key
    fn build_key(route: &str, key: &str) -> Vec<u8> {
        format!("{}:{}", route, key).into_bytes()
    }

    /// Process a KV operation with route-based key namespacing
    pub async fn handle_operation(
        &self,
        operation: KvOperation,
        route: &str,
        key: Option<String>,
        value: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        match operation {
            KvOperation::Put => self.handle_put(route, key, value).await,
            KvOperation::Get => self.handle_get(route, key).await,
            KvOperation::Delete => self.handle_delete(route, key).await,
            KvOperation::Scan => self.handle_scan(route, key).await,
            KvOperation::Batch => self.handle_batch(route, value).await,
            KvOperation::GetMany => self.handle_get_many(route, value).await,
            KvOperation::DeleteRange => self.handle_delete_range(route, key, value).await,
        }
    }

    /// Handle put operation: store key-value pair
    async fn handle_put(
        &self,
        route: &str,
        key: Option<String>,
        value: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        let key = key.ok_or_else(|| "PUT requires a key".to_string())?;
        let value = value.ok_or_else(|| "PUT requires a value".to_string())?;

        let namespaced_key = Self::build_key(route, &key);
        self.store
            .put(&namespaced_key, &value)
            .map_err(|e| e.to_string())?;
        Ok(None)
    }

    /// Handle get operation: retrieve value by key
    async fn handle_get(
        &self,
        route: &str,
        key: Option<String>,
    ) -> Result<Option<Vec<u8>>, String> {
        let key = key.ok_or_else(|| "GET requires a key".to_string())?;

        let namespaced_key = Self::build_key(route, &key);
        match self.store.get(&namespaced_key).map_err(|e| e.to_string())? {
            Some(bytes) => Ok(Some(bytes.to_vec())),
            None => Ok(None),
        }
    }

    /// Handle delete operation: remove key
    async fn handle_delete(
        &self,
        route: &str,
        key: Option<String>,
    ) -> Result<Option<Vec<u8>>, String> {
        let key = key.ok_or_else(|| "DELETE requires a key".to_string())?;

        let namespaced_key = Self::build_key(route, &key);
        self.store
            .delete(&namespaced_key)
            .map_err(|e| e.to_string())?;
        Ok(None)
    }

    /// Handle scan operation: list keys with prefix
    async fn handle_scan(
        &self,
        route: &str,
        prefix: Option<String>,
    ) -> Result<Option<Vec<u8>>, String> {
        let start_key = prefix.unwrap_or_default();

        let start_bytes = Self::build_key(route, &start_key);
        // Create end bytes by incrementing the last byte (simple prefix scan)
        let mut end_bytes = start_bytes.clone();
        if let Some(last) = end_bytes.last_mut() {
            *last = last.saturating_add(1);
        }

        let results = self
            .store
            .scan(&start_bytes, &end_bytes)
            .map_err(|e| e.to_string())?;

        // Convert results to TLV format, removing route prefix from keys
        let keys: Vec<String> = results
            .into_iter()
            .take(100) // Limit to 100 results
            .map(|(k, _)| {
                // Remove the route: prefix
                String::from_utf8_lossy(&k)
                    .strip_prefix(&format!("{}:", route))
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        let response = keys.join("\n");
        Ok(Some(response.into_bytes()))
    }

    /// Handle batch operation: atomic multi-operation transaction
    /// Body format: newline-separated operations
    /// Each line: "PUT key value" | "DELETE key"
    async fn handle_batch(
        &self,
        route: &str,
        body: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        let body = body.ok_or_else(|| "Batch requires body with operations".to_string())?;
        let operations =
            String::from_utf8(body).map_err(|_| "Batch body must be UTF-8".to_string())?;

        // Parse operations into separate put and delete lists
        let mut puts = Vec::new();

        for line in operations.lines() {
            let parts: Vec<&str> = line.splitn(3, ' ').collect();
            match parts.as_slice() {
                ["PUT", key, value] => {
                    let namespaced_key = Self::build_key(route, key);
                    puts.push((namespaced_key, value.as_bytes().to_vec()));
                }
                ["DELETE", key] => {
                    let namespaced_key = Self::build_key(route, key);
                    self.store
                        .delete(&namespaced_key)
                        .map_err(|e| e.to_string())?;
                }
                _ => return Err(format!("Invalid batch operation: {}", line)),
            }
        }

        // Execute puts in batch
        if !puts.is_empty() {
            self.store.put_batch(puts).map_err(|e| e.to_string())?;
        }

        // Return empty response on success
        Ok(None)
    }

    /// Handle get-many operation: retrieve multiple keys
    /// Body format: newline-separated keys
    /// Response: length-prefixed values
    async fn handle_get_many(
        &self,
        route: &str,
        body: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        let body = body.ok_or_else(|| "GetMany requires body with keys".to_string())?;
        let keys_str =
            String::from_utf8(body).map_err(|_| "GetMany body must be UTF-8".to_string())?;

        let keys: Vec<String> = keys_str.lines().map(|s| s.to_string()).collect();

        // Get values individually
        let mut response = Vec::new();
        for key in keys {
            let namespaced_key = Self::build_key(route, &key);
            match self.store.get(&namespaced_key).map_err(|e| e.to_string())? {
                Some(bytes) => {
                    let value = bytes.to_vec();
                    let len = u32::try_from(value.len())
                        .map_err(|_| "Value too large")?
                        .to_be_bytes();
                    response.extend_from_slice(&len);
                    response.extend_from_slice(&value);
                }
                None => {
                    // Encode empty value as length 0
                    response.extend_from_slice(&[0, 0, 0, 0]);
                }
            }
        }

        Ok(Some(response))
    }

    /// Handle delete-range operation: remove keys in range [start, end)
    /// TAG_ID = start_key, TAG_BODY = end_key
    async fn handle_delete_range(
        &self,
        route: &str,
        start_key: Option<String>,
        end_key_bytes: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        let start =
            start_key.ok_or_else(|| "DeleteRange requires start key (TAG_ID)".to_string())?;
        let end_bytes =
            end_key_bytes.ok_or_else(|| "DeleteRange requires end key (TAG_BODY)".to_string())?;
        let end = String::from_utf8(end_bytes).map_err(|_| "End key must be UTF-8".to_string())?;

        let start_key_bytes = Self::build_key(route, &start);
        let end_key_bytes_full = Self::build_key(route, &end);

        // Scan the range to get all keys
        let items = self
            .store
            .scan(&start_key_bytes, &end_key_bytes_full)
            .map_err(|e| e.to_string())?;
        let keys_to_delete: Vec<Vec<u8>> = items.into_iter().map(|(k, _)| k.to_vec()).collect();

        if !keys_to_delete.is_empty() {
            self.store
                .delete_batch(keys_to_delete)
                .map_err(|e| e.to_string())?;
        }

        // Return empty response on success
        Ok(None)
    }
}
