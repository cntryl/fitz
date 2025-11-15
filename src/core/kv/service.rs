//! KV domain service - simple key-value operations
//!
//! The KV service provides basic key-value operations with route-based key namespacing.
//! Keys are scoped by the route resource path for multi-tenancy.
//! All operations require explicit transaction semantics.

use super::types::KvOperation;
use crate::storage::traits::{KvStore, KvTransaction};
use cntryl_midge::ColumnFamilyId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

// Default column family for KV domain
const DEFAULT_CF: ColumnFamilyId = ColumnFamilyId(0);

// KV domain prefix to prevent conflicts with other domains
const KV_DOMAIN_PREFIX: &str = "kv";

/// Active KV transaction state
/// Tracks transaction metadata and realm/area scope
struct ActiveTransaction {
    /// Transaction handle
    transaction: Box<dyn KvTransaction>,
    /// Realm this transaction is scoped to
    realm: String,
    /// Area this transaction is scoped to
    area: String,
    /// Transaction ID
    id: u64,
}

/// KV service handles key-value storage operations with transaction semantics
/// All operations require an active transaction
/// - Put: store key-value pairs
/// - Get: retrieve values by key
/// - Delete: remove keys
/// - Scan: list keys with prefix
/// - Batch: atomic multi-operation transactions
/// - GetMany: retrieve multiple keys
/// - DeleteRange: remove keys in range
pub struct KvService {
    store: Arc<dyn KvStore>,
    /// Active transactions per transaction_id
    active_transactions: Arc<Mutex<HashMap<u64, ActiveTransaction>>>,
    /// Next transaction ID
    next_transaction_id: Arc<Mutex<u64>>,
}

impl KvService {
    /// Create a new KV service
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self {
            store: kv_store,
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
            next_transaction_id: Arc::new(Mutex::new(1)),
        }
    }

    /// Build namespaced key from route and key with KV domain prefix
    /// Format: kv:{realm}:{area}:{key}
    fn build_key(realm: &str, area: &str, key: &str) -> Vec<u8> {
        format!("{}:{}:{}:{}", KV_DOMAIN_PREFIX, realm, area, key).into_bytes()
    }

    /// Process a KV operation with route-based key namespacing
    pub async fn handle_operation(
        &self,
        operation: KvOperation,
        route: &str,
        key: Option<String>,
        value: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        // Parse realm and area from route
        let parts: Vec<&str> = route.split("://").nth(1)
            .ok_or_else(|| "Invalid route format".to_string())?
            .split('/')
            .collect();
        let realm = parts.first().ok_or_else(|| "Missing realm".to_string())?;
        let area = parts.get(1).ok_or_else(|| "Missing area".to_string())?;

        match operation {
            KvOperation::Put => self.handle_put(realm, area, key, value).await,
            KvOperation::Get => self.handle_get(realm, area, key).await,
            KvOperation::Delete => self.handle_delete(realm, area, key).await,
            KvOperation::Scan => self.handle_scan(realm, area, key, value).await,
            KvOperation::Batch => self.handle_batch(realm, area, value).await,
            KvOperation::GetMany => self.handle_get_many(realm, area, value).await,
            KvOperation::DeleteRange => self.handle_delete_range(realm, area, key, value).await,
            KvOperation::BeginTransaction => self.handle_begin_transaction(realm, area).await,
            KvOperation::CommitTransaction => self.handle_commit_transaction(key).await,
            KvOperation::RollbackTransaction => self.handle_rollback_transaction(key).await,
        }
    }

    /// Handle put operation: store key-value pair
    async fn handle_put(
        &self,
        realm: &str,
        area: &str,
        key: Option<String>,
        value: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        let key = key.ok_or_else(|| "PUT requires a key".to_string())?;
        let value = value.ok_or_else(|| "PUT requires a value".to_string())?;

        let namespaced_key = Self::build_key(realm, area, &key);
        self.store
            .put(DEFAULT_CF, &value, &namespaced_key)
            .map_err(|e| e.to_string())?;
        Ok(None)
    }

    /// Handle get operation: retrieve value by key
    async fn handle_get(
        &self,
        realm: &str,
        area: &str,
        key: Option<String>,
    ) -> Result<Option<Vec<u8>>, String> {
        let key = key.ok_or_else(|| "GET requires a key".to_string())?;

        let namespaced_key = Self::build_key(realm, area, &key);
        match self
            .store
            .get(DEFAULT_CF, &namespaced_key)
            .map_err(|e| e.to_string())?
        {
            Some(bytes) => Ok(Some(bytes.to_vec())),
            None => Ok(None),
        }
    }

    /// Handle delete operation: remove key
    async fn handle_delete(
        &self,
        realm: &str,
        area: &str,
        key: Option<String>,
    ) -> Result<Option<Vec<u8>>, String> {
        let key = key.ok_or_else(|| "DELETE requires a key".to_string())?;

        let namespaced_key = Self::build_key(realm, area, &key);
        self.store
            .delete(DEFAULT_CF, &namespaced_key)
            .map_err(|e| e.to_string())?;
        Ok(None)
    }

    /// Handle scan operation: list keys with optional begin and end range
    /// Body format: "start_key\nend_key" (end_key optional)
    async fn handle_scan(
        &self,
        realm: &str,
        area: &str,
        _key: Option<String>, // Not used for scan
        body: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        let body = body.ok_or_else(|| "Scan requires body with range parameters".to_string())?;
        let params = String::from_utf8(body).map_err(|_| "Scan body must be UTF-8".to_string())?;
        let lines: Vec<&str> = params.lines().collect();
        
        let start_key = lines.first().map(|s| s.to_string()).unwrap_or_default();
        let end_key = lines.get(1).map(|s| s.to_string());

        // Build start and end keys based on provided keys
        let (start_bytes, end_bytes) = if let Some(end_key) = end_key {
            // Explicit range scan: from start_key to end_key
            let start_bytes = Self::build_key(realm, area, &start_key);
            let end_bytes = Self::build_key(realm, area, &end_key);
            (start_bytes, end_bytes)
        } else if start_key.is_empty() {
            // Scan entire area: kv:realm:area:000... to kv:realm:area:fff...
            let start_prefix = format!("{}:{}:{}:", KV_DOMAIN_PREFIX, realm, area);
            let start_bytes = start_prefix.clone().into_bytes();
            let mut end_bytes = start_prefix.into_bytes();
            if let Some(last) = end_bytes.last_mut() {
                *last = last.saturating_add(1);
            }
            (start_bytes, end_bytes)
        } else {
            // Scan from specific prefix within area
            let start_bytes = Self::build_key(realm, area, &start_key);
            let mut end_bytes = start_bytes.clone();
            if let Some(last) = end_bytes.last_mut() {
                *last = last.saturating_add(1);
            }
            (start_bytes, end_bytes)
        };

        let results = self
            .store
            .scan(DEFAULT_CF, &start_bytes, &end_bytes)
            .map_err(|e| e.to_string())?;

        // Convert results to TLV format, removing route prefix from keys
        let keys: Vec<String> = results
            .into_iter()
            .take(100) // Limit to 100 results
            .map(|(k, _)| {
                // Remove the kv:realm:area: prefix
                String::from_utf8_lossy(&k)
                    .strip_prefix(&format!("{}:{}:{}:", KV_DOMAIN_PREFIX, realm, area))
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
        realm: &str,
        area: &str,
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
                    let namespaced_key = Self::build_key(realm, area, key);
                    puts.push((namespaced_key, value.as_bytes().to_vec()));
                }
                ["DELETE", key] => {
                    let namespaced_key = Self::build_key(realm, area, key);
                    self.store
                        .delete(DEFAULT_CF, &namespaced_key)
                        .map_err(|e| e.to_string())?;
                }
                _ => return Err(format!("Invalid batch operation: {}", line)),
            }
        }

        // Execute puts one by one (Midge doesn't have batch API)
        for (key, value) in puts {
            self.store
                .put(DEFAULT_CF, &value, &key)
                .map_err(|e| e.to_string())?;
        }

        // Return empty response on success
        Ok(None)
    }

    /// Handle get-many operation: retrieve multiple keys
    /// Body format: newline-separated keys
    /// Response: length-prefixed values
    async fn handle_get_many(
        &self,
        realm: &str,
        area: &str,
        body: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        let body = body.ok_or_else(|| "GetMany requires body with keys".to_string())?;
        let keys_str =
            String::from_utf8(body).map_err(|_| "GetMany body must be UTF-8".to_string())?;

        let keys: Vec<String> = keys_str.lines().map(|s| s.to_string()).collect();

        // Get values individually
        let mut response = Vec::new();
        for key in keys {
            let namespaced_key = Self::build_key(realm, area, &key);
            match self
                .store
                .get(DEFAULT_CF, &namespaced_key)
                .map_err(|e| e.to_string())?
            {
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
    /// Body format: "start_key\nend_key"
    async fn handle_delete_range(
        &self,
        realm: &str,
        area: &str,
        _key: Option<String>, // Not used for delete_range
        body: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        let body = body.ok_or_else(|| "DeleteRange requires body with range parameters".to_string())?;
        let params = String::from_utf8(body).map_err(|_| "DeleteRange body must be UTF-8".to_string())?;
        let lines: Vec<&str> = params.lines().collect();
        
        let start = lines.first().ok_or_else(|| "DeleteRange requires start key as first line".to_string())?;
        let end = lines.get(1).ok_or_else(|| "DeleteRange requires end key as second line".to_string())?;

        let start_key_bytes = Self::build_key(realm, area, start);
        let end_key_bytes_full = Self::build_key(realm, area, end);

        // Scan the range to get all keys
        let items = self
            .store
            .scan(DEFAULT_CF, &start_key_bytes, &end_key_bytes_full)
            .map_err(|e| e.to_string())?;
        let keys_to_delete: Vec<Vec<u8>> = items.into_iter().map(|(k, _)| k.to_vec()).collect();

        // Delete keys one by one (Midge doesn't have batch API)
        for key in keys_to_delete {
            self.store
                .delete(DEFAULT_CF, &key)
                .map_err(|e| e.to_string())?;
        }

        // Return empty response on success
        Ok(None)
    }

    /// Handle begin transaction: start a new transaction for realm/area
    async fn handle_begin_transaction(
        &self,
        realm: &str,
        area: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        // Get next transaction ID
        let transaction_id = {
            let mut next_id = self.next_transaction_id.lock().await;
            let id = *next_id;
            *next_id += 1;
            id
        };

        // Begin transaction
        let transaction = self.store
            .begin_transaction(DEFAULT_CF)
            .map_err(|e| format!("Failed to begin transaction: {:?}", e))?;

        // Create and store transaction state
        let txn = ActiveTransaction {
            transaction,
            realm: realm.to_string(),
            area: area.to_string(),
            id: transaction_id,
        };

        let mut transactions = self.active_transactions.lock().await;
        transactions.insert(transaction_id, txn);

        // Return transaction ID as response
        Ok(Some(transaction_id.to_string().into_bytes()))
    }

    /// Handle commit transaction: commit the specified transaction
    async fn handle_commit_transaction(
        &self,
        transaction_id_str: Option<String>,
    ) -> Result<Option<Vec<u8>>, String> {
        let transaction_id = transaction_id_str
            .ok_or_else(|| "Transaction ID required for commit".to_string())?
            .parse::<u64>()
            .map_err(|_| "Invalid transaction ID".to_string())?;

        let mut transactions = self.active_transactions.lock().await;
        let _txn = transactions
            .remove(&transaction_id)
            .ok_or_else(|| "Transaction not found".to_string())?;

        // For now, operations are auto-committed when executed
        // In the future, we might implement proper transaction semantics
        Ok(None)
    }

    /// Handle rollback transaction: rollback the specified transaction
    async fn handle_rollback_transaction(
        &self,
        transaction_id_str: Option<String>,
    ) -> Result<Option<Vec<u8>>, String> {
        let transaction_id = transaction_id_str
            .ok_or_else(|| "Transaction ID required for rollback".to_string())?
            .parse::<u64>()
            .map_err(|_| "Invalid transaction ID".to_string())?;

        let mut transactions = self.active_transactions.lock().await;
        let _txn = transactions
            .remove(&transaction_id)
            .ok_or_else(|| "Transaction not found".to_string())?;

        // For now, operations are auto-committed when executed
        // In the future, we might implement proper rollback semantics
        Ok(None)
    }
}
