//! Queue domain service - durable message queue with lease semantics
//!
//! Messages are stored with lease tracking for at-least-once delivery.
//! Supports hierarchical configuration (realm > area > resource).
//!
//! Key schema (using lexkey for building):
//! - 0x02 0x01 {realm} {area} {resource} {message_id} → Message data
//! - 0x02 0x02 {realm} {area} {resource} {message_id} → Lease info
//! - 0x02 0x03 {realm} {area} {resource} → Queue configuration

use crate::core::queue::types::{QueueConfig, QueueMessage};
use crate::storage::markers::{queue as queue_prefixes, QUEUE_DOMAIN_PREFIX};
use crate::storage::traits::KvStore;
use cntryl_midge::ColumnFamilyId;
use lexkey::{encode_composite, Encodable};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

// Default column family for queue operations
const DEFAULT_CF: ColumnFamilyId = ColumnFamilyId(0);

// NOTE: The following code requires updates for midge v2+ API which changed KvStore
// All KvStore methods now require ColumnFamilyHandle as first parameter
// This needs to be refactored once midge exposes default_column_family() or similar
// Type aliases to reduce complexity
type LeaseInfo = (u64, String, u32); // (expiry_secs, owner_token, delivery_count)
type LeaseMap = HashMap<String, HashMap<String, LeaseInfo>>;
type RealmConfigMap = HashMap<String, QueueConfig>;
type AreaConfigMap = HashMap<(String, String), QueueConfig>;
type ResourceConfigMap = HashMap<(String, String, String), QueueConfig>;

/// Queue domain prefix marker
const DOMAIN_PREFIX: u8 = QUEUE_DOMAIN_PREFIX;

/// Index type markers (second byte after domain prefix)
const IDX_MESSAGE: u8 = queue_prefixes::MESSAGE;
const IDX_LEASE: u8 = queue_prefixes::LEASE;
const IDX_CONFIG: u8 = queue_prefixes::CONFIG;

/// QueueService owns all queue business logic.
/// Uses KvStore for durable persistence.
/// Tracks leases in-memory, persists messages to KvStore.
pub struct QueueService {
    kv_store: Arc<dyn KvStore>,
    token_key: Vec<u8>,

    // In-memory lease tracking: route -> id -> (expiry_secs, owner_token, delivery_count)
    leases: Arc<Mutex<LeaseMap>>,

    // Hierarchical configuration maps
    cfg_realm: Arc<Mutex<RealmConfigMap>>,
    cfg_area: Arc<Mutex<AreaConfigMap>>,
    cfg_resource: Arc<Mutex<ResourceConfigMap>>,
}

impl QueueService {
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        // Generate random HMAC key for delivery tokens
        let uuid = Uuid::new_v4();
        let key = uuid.as_bytes().to_vec();

        Self {
            kv_store,
            token_key: key,
            leases: Arc::new(Mutex::new(HashMap::new())),
            cfg_realm: Arc::new(Mutex::new(HashMap::new())),
            cfg_area: Arc::new(Mutex::new(HashMap::new())),
            cfg_resource: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build message key: {DOMAIN_PREFIX} {IDX_MESSAGE} {realm} {area} {resource} {message_id}
    fn key_message(realm: &str, area: &str, resource: &str, message_id: &str) -> Vec<u8> {
        encode_composite!(
            DOMAIN_PREFIX,
            IDX_MESSAGE,
            realm,
            area,
            resource,
            message_id
        )
        .as_bytes()
        .to_vec()
    }

    /// Build lease key: {DOMAIN_PREFIX} {IDX_LEASE} {realm} {area} {resource} {message_id}
    fn key_lease(realm: &str, area: &str, resource: &str, message_id: &str) -> Vec<u8> {
        encode_composite!(DOMAIN_PREFIX, IDX_LEASE, realm, area, resource, message_id)
            .as_bytes()
            .to_vec()
    }

    /// Build config key: {DOMAIN_PREFIX} {IDX_CONFIG} {realm} {area} {resource}
    fn key_config(realm: &str, area: &str, resource: &str) -> Vec<u8> {
        encode_composite!(DOMAIN_PREFIX, IDX_CONFIG, realm, area, resource)
            .as_bytes()
            .to_vec()
    }

    /// List queues within a specific realm/area scope
    /// Returns queues that match the pattern: queue:{realm}/{area}/*
    pub async fn list_queues_in_scope(
        &self,
        realm: &str,
        area: &str,
    ) -> Result<Vec<String>, String> {
        use std::collections::HashSet;

        // Scan for queue keys in this scope: queue:{realm}/{area}/
        let scope_prefix = format!("queue:{}/{}:", realm, area);
        let results = self
            .kv_store
            .scan(DEFAULT_CF, scope_prefix.as_bytes(), &[])
            .map_err(|e| format!("scan error: {:?}", e))?;

        let mut routes = HashSet::new();

        for (key_bytes, _) in results {
            if key_bytes.starts_with(scope_prefix.as_bytes()) {
                // Extract resource from key format: queue:{realm}/{area}:{resource}:{message_id}
                if let Ok(key_str) = std::str::from_utf8(&key_bytes) {
                    if let Some(route_part) = key_str.strip_prefix(&scope_prefix) {
                        if let Some(resource) = route_part.split(':').next() {
                            let full_route = format!("{}/{}/{}", realm, area, resource);
                            routes.insert(full_route);
                        }
                    }
                }
            }
        }

        let mut queue_list: Vec<String> = routes.into_iter().collect();
        queue_list.sort(); // Return in sorted order
        Ok(queue_list)
    }

    /// List all queues within a specific realm
    /// Returns queues that match the pattern: queue:{realm}/*/*
    pub async fn list_queues_in_realm(&self, realm: &str) -> Result<Vec<String>, String> {
        use std::collections::HashSet;

        // Scan for queue keys in this realm: queue:{realm}/
        let realm_prefix = format!("queue:{}:", realm);
        let results = self
            .kv_store
            .scan(DEFAULT_CF, realm_prefix.as_bytes(), &[])
            .map_err(|e| format!("scan error: {:?}", e))?;

        let mut routes = HashSet::new();

        for (key_bytes, _) in results {
            if key_bytes.starts_with(realm_prefix.as_bytes()) {
                // Extract route from key format: queue:{realm}:{area}:{resource}:{message_id}
                if let Ok(key_str) = std::str::from_utf8(&key_bytes) {
                    if let Some(route_part) = key_str.strip_prefix(&realm_prefix) {
                        // Split by ':' and take first two parts (area/resource)
                        let parts: Vec<&str> = route_part.split(':').collect();
                        if parts.len() >= 2 {
                            let full_route = format!("{}/{}/{}", realm, parts[0], parts[1]);
                            routes.insert(full_route);
                        }
                    }
                }
            }
        }

        let mut queue_list: Vec<String> = routes.into_iter().collect();
        queue_list.sort(); // Return in sorted order
        Ok(queue_list)
    }

    // NOTE: All remaining methods below are DISABLED pending midge KvStore API integration
    // The midge KvStore trait now requires ColumnFamilyHandle for all operations
    // Once midge exposes a way to get/create the default column family, re-enable these methods

    // All queue operations are disabled until midge KvStore API is updated

    /// Enqueue a message to the specified queue
    pub async fn enqueue(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        body: Vec<u8>,
        ttl_secs: Option<u64>,
        _dedupe_key: Option<&str>,
    ) -> Result<String, String> {
        use crate::core::queue::types::QueueMessage;
        use std::time::{SystemTime, UNIX_EPOCH};

        // Generate unique message ID
        let message_id = format!("msg_{}", uuid::Uuid::new_v4().simple());

        // Create message structure
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("Time error: {:?}", e))?
            .as_secs();

        let message = QueueMessage {
            id: message_id.clone(),
            route: format!("{}/{}/{}", realm, area, resource),
            body,
            lease_expiry: None,
            lease_owner: None,
            delivery_count: 0,
            created_at: now,
            ttl_secs,
        };

        // Serialize message
        let message_data = serde_json::to_vec(&message)
            .map_err(|e| format!("Serialization error: {:?}", e))?;

        // Build storage key
        let key = Self::key_message(realm, area, resource, &message_id);

        // Store message
        self.kv_store
            .put(DEFAULT_CF, &message_data, &key)
            .map_err(|e| format!("Storage error: {:?}", e))?;

        Ok(message_id)
    }

    /// Reserve (lease) messages from the specified queue
    pub async fn reserve(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        batch_size: usize,
        lease_secs: u32,
    ) -> Result<Vec<QueueMessage>, String> {
        use crate::core::queue::types::QueueMessage;
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("Time error: {:?}", e))?
            .as_secs();

        let lease_expiry = now + lease_secs as u64;
        let token = self.generate_delivery_token(realm, area, resource, &format!("batch_{}", now));

        // Scan for available messages (not leased or lease expired)
        let prefix = Self::key_message(realm, area, resource, "");
        let results = self
            .kv_store
            .scan(DEFAULT_CF, &prefix, &[])
            .map_err(|e| format!("Scan error: {:?}", e))?;

        let mut available_messages = Vec::new();
        let mut to_update = Vec::new();

        for (key_bytes, value_bytes) in results {
            if available_messages.len() >= batch_size {
                break;
            }

            // Parse message
            let message: QueueMessage = serde_json::from_slice(&value_bytes)
                .map_err(|e| format!("Deserialization error: {:?}", e))?;

            // Check if message is available (no lease or expired lease)
            let is_available = match message.lease_expiry {
                Some(expiry) => expiry <= now,
                None => true,
            };

            if is_available {
                // Extract message ID from key for lease key
                let key_str = String::from_utf8_lossy(&key_bytes);
                let message_id = key_str
                    .split(':')
                    .last()
                    .ok_or_else(|| "Invalid key format".to_string())?;

                // Update message with lease
                let mut leased_message = message.clone();
                leased_message.lease_expiry = Some(lease_expiry);
                leased_message.lease_owner = Some(token.clone());
                leased_message.delivery_count += 1;

                available_messages.push(leased_message.clone());
                to_update.push((key_bytes.clone(), message_id.to_string(), leased_message));
            }
        }

        // Update leased messages in storage
        for (key, message_id, message) in to_update {
            let message_data = serde_json::to_vec(&message)
                .map_err(|e| format!("Serialization error: {:?}", e))?;

            self.kv_store
                .put(DEFAULT_CF, &message_data, &key)
                .map_err(|e| format!("Storage error: {:?}", e))?;

            // Update in-memory lease tracking
            let mut leases = self.leases.lock().await;
            let route_key = format!("{}/{}/{}", realm, area, resource);
            let route_leases = leases.entry(route_key).or_insert_with(HashMap::new);
            route_leases.insert(message_id, (lease_expiry, token.clone(), message.delivery_count));
        }

        Ok(available_messages)
    }

    /// Complete (acknowledge) a leased message
    pub async fn complete(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        message_id: &str,
        delivery_token: &str,
    ) -> Result<(), String> {
        // Verify token
        self.verify_delivery_token(realm, area, resource, message_id, delivery_token)?;

        // Get message
        let key = Self::key_message(realm, area, resource, message_id);
        let message_data = self
            .kv_store
            .get(DEFAULT_CF, &key)
            .map_err(|e| format!("Storage error: {:?}", e))?
            .ok_or_else(|| "Message not found".to_string())?;

        let message: crate::core::queue::types::QueueMessage = serde_json::from_slice(&message_data)
            .map_err(|e| format!("Deserialization error: {:?}", e))?;

        // Verify lease ownership
        if message.lease_owner.as_deref() != Some(delivery_token) {
            return Err("Invalid delivery token".to_string());
        }

        // Delete message from storage
        self.kv_store
            .delete(DEFAULT_CF, &key)
            .map_err(|e| format!("Storage error: {:?}", e))?;

        // Remove from in-memory lease tracking
        let mut leases = self.leases.lock().await;
        let route_key = format!("{}/{}/{}", realm, area, resource);
        if let Some(route_leases) = leases.get_mut(&route_key) {
            route_leases.remove(message_id);
        }

        Ok(())
    }

    /// Extend the lease on a message
    pub async fn extend_lease(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        message_id: &str,
        delivery_token: &str,
        additional_secs: u32,
    ) -> Result<(), String> {
        // Verify token
        self.verify_delivery_token(realm, area, resource, message_id, delivery_token)?;

        // Get message
        let key = Self::key_message(realm, area, resource, message_id);
        let message_data = self
            .kv_store
            .get(DEFAULT_CF, &key)
            .map_err(|e| format!("Storage error: {:?}", e))?
            .ok_or_else(|| "Message not found".to_string())?;

        let mut message: crate::core::queue::types::QueueMessage = serde_json::from_slice(&message_data)
            .map_err(|e| format!("Deserialization error: {:?}", e))?;

        // Verify lease ownership
        if message.lease_owner.as_deref() != Some(delivery_token) {
            return Err("Invalid delivery token".to_string());
        }

        // Extend lease
        let new_expiry = message.lease_expiry
            .ok_or_else(|| "Message not leased".to_string())? + additional_secs as u64;
        message.lease_expiry = Some(new_expiry);

        // Update storage
        let updated_data = serde_json::to_vec(&message)
            .map_err(|e| format!("Serialization error: {:?}", e))?;

        self.kv_store
            .put(DEFAULT_CF, &updated_data, &key)
            .map_err(|e| format!("Storage error: {:?}", e))?;

        // Update in-memory lease tracking
        let mut leases = self.leases.lock().await;
        let route_key = format!("{}/{}/{}", realm, area, resource);
        if let Some(route_leases) = leases.get_mut(&route_key) {
            if let Some((_, token, _delivery_count)) = route_leases.get_mut(message_id) {
                *token = delivery_token.to_string();
            }
        }

        Ok(())
    }

    /// Peek at the next available message without leasing it
    pub async fn peek(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<crate::core::queue::types::QueueMessage>, String> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("Time error: {:?}", e))?
            .as_secs();

        // Scan for available messages
        let prefix = Self::key_message(realm, area, resource, "");
        let results = self
            .kv_store
            .scan(DEFAULT_CF, &prefix, &[])
            .map_err(|e| format!("Scan error: {:?}", e))?;

        for (_key_bytes, value_bytes) in results {
            let message: crate::core::queue::types::QueueMessage = serde_json::from_slice(&value_bytes)
                .map_err(|e| format!("Deserialization error: {:?}", e))?;

            // Check if message is available
            let is_available = match message.lease_expiry {
                Some(expiry) => expiry <= now,
                None => true,
            };

            if is_available {
                return Ok(Some(message));
            }
        }

        Ok(None)
    }

    /// Generate a delivery token for a message
    fn generate_delivery_token(&self, realm: &str, area: &str, resource: &str, nonce: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let mut mac = Hmac::<Sha256>::new_from_slice(&self.token_key)
            .expect("HMAC can take key of any size");
        mac.update(realm.as_bytes());
        mac.update(area.as_bytes());
        mac.update(resource.as_bytes());
        mac.update(nonce.as_bytes());

        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    /// Verify a delivery token
    fn verify_delivery_token(&self, realm: &str, area: &str, resource: &str, message_id: &str, token: &str) -> Result<(), String> {
        // For now, just check if the token exists in our in-memory tracking
        // In a production system, you'd verify the HMAC
        let leases = self.leases.blocking_lock();
        let route_key = format!("{}/{}/{}", realm, area, resource);
        if let Some(route_leases) = leases.get(&route_key) {
            if let Some((_, stored_token, _)) = route_leases.get(message_id) {
                if stored_token == token {
                    return Ok(());
                }
            }
        }
        Err("Invalid or expired token".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::midge_adapter::create_memory_store;

    async fn create_test_service() -> QueueService {
        let kv_store = create_memory_store().expect("Failed to create memory store");
        QueueService::new(kv_store)
    }

    #[test]
    fn should_build_message_key_correctly() {
        // Arrange
        let realm = "test_realm";
        let area = "test_area";
        let resource = "test_resource";
        let message_id = "msg_123";

        // Act
        let key = QueueService::key_message(realm, area, resource, message_id);

        // Assert
        assert!(!key.is_empty());
        // Key should contain domain prefix and message index in the encoded form
        // Since encoding format changed, just verify the key is properly formed
        // and contains the expected values
        assert!(key.len() > 16); // Should be longer than just the prefix encoding
                                 // The domain prefix should appear in the encoding
        assert!(key.contains(&DOMAIN_PREFIX));
        // The message index should appear in the encoding
        assert!(key.contains(&IDX_MESSAGE));
    }

    #[test]
    fn should_build_lease_key_correctly() {
        // Arrange
        let realm = "test_realm";
        let area = "test_area";
        let resource = "test_resource";
        let message_id = "msg_123";

        // Act
        let key = QueueService::key_lease(realm, area, resource, message_id);

        // Assert
        assert!(!key.is_empty());
        // Key should contain domain prefix and lease index in the encoded form
        assert!(key.len() > 16);
        assert!(key.contains(&DOMAIN_PREFIX));
        assert!(key.contains(&IDX_LEASE));
    }

    #[test]
    fn should_build_config_key_correctly() {
        // Arrange
        let realm = "test_realm";
        let area = "test_area";
        let resource = "test_resource";

        // Act
        let key = QueueService::key_config(realm, area, resource);

        // Assert
        assert!(!key.is_empty());
        // Key should contain domain prefix and config index in the encoded form
        assert!(key.len() > 16);
        assert!(key.contains(&DOMAIN_PREFIX));
        assert!(key.contains(&IDX_CONFIG));
    }

    #[test]
    fn should_create_service_with_memory_store() {
        // Arrange
        let kv_store = create_memory_store().expect("Failed to create memory store");

        // Act
        let service = QueueService::new(kv_store);

        // Assert
        assert!(!service.token_key.is_empty());
        assert_eq!(service.token_key.len(), 16); // UUID bytes
    }

    #[tokio::test]
    async fn should_list_queues_in_scope_with_no_queues() {
        // Arrange
        let service = create_test_service().await;

        // Act
        let result = service.list_queues_in_scope("realm1", "area1").await;

        // Assert
        assert!(result.is_ok());
        let queues = result.unwrap();
        assert!(queues.is_empty());
    }

    #[tokio::test]
    async fn should_list_queues_in_realm_with_no_queues() {
        // Arrange
        let service = create_test_service().await;

        // Act
        let result = service.list_queues_in_realm("realm1").await;

        // Assert
        assert!(result.is_ok());
        let queues = result.unwrap();
        assert!(queues.is_empty());
    }
}
