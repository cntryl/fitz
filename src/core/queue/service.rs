//! Queue domain service - durable message queue with lease semantics
//!
//! Messages are stored with lease tracking for at-least-once delivery.
//! Supports hierarchical configuration (realm > area > resource).
//!
//! Key schema (using lexkey for building):
//! - 0x02 0x01 {realm} {area} {resource} {message_id} → Message data
//! - 0x02 0x02 {realm} {area} {resource} {message_id} → Lease info
//! - 0x02 0x03 {realm} {area} {resource} → Queue configuration

use crate::core::queue::types::QueueConfig;
use crate::storage::markers::{queue as queue_prefixes, QUEUE_DOMAIN_PREFIX};
use crate::storage::traits::KvStore;
use cntryl_midge::ColumnFamilyId;
use lexkey;
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
        lexkey::LexKey::encode_composite(&[
            &[DOMAIN_PREFIX, IDX_MESSAGE],
            realm.as_bytes(),
            area.as_bytes(),
            resource.as_bytes(),
            message_id.as_bytes(),
        ])
        .as_bytes()
        .to_vec()
    }

    /// Build lease key: {DOMAIN_PREFIX} {IDX_LEASE} {realm} {area} {resource} {message_id}
    fn key_lease(realm: &str, area: &str, resource: &str, message_id: &str) -> Vec<u8> {
        lexkey::LexKey::encode_composite(&[
            &[DOMAIN_PREFIX, IDX_LEASE],
            realm.as_bytes(),
            area.as_bytes(),
            resource.as_bytes(),
            message_id.as_bytes(),
        ])
        .as_bytes()
        .to_vec()
    }

    /// Build config key: {DOMAIN_PREFIX} {IDX_CONFIG} {realm} {area} {resource}
    fn key_config(realm: &str, area: &str, resource: &str) -> Vec<u8> {
        lexkey::LexKey::encode_composite(&[
            &[DOMAIN_PREFIX, IDX_CONFIG],
            realm.as_bytes(),
            area.as_bytes(),
            resource.as_bytes(),
        ])
        .as_bytes()
        .to_vec()
    }

    /// List queues within a specific realm/area scope
    /// Returns queues that match the pattern: queue:{realm}/{area}/*
    pub async fn list_queues_in_scope(&self, realm: &str, area: &str) -> Result<Vec<String>, String> {
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
        // Key should start with domain prefix and message index
        assert_eq!(key[0], DOMAIN_PREFIX);
        assert_eq!(key[1], IDX_MESSAGE);
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
        // Key should start with domain prefix and lease index
        assert_eq!(key[0], DOMAIN_PREFIX);
        assert_eq!(key[1], IDX_LEASE);
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
        // Key should start with domain prefix and config index
        assert_eq!(key[0], DOMAIN_PREFIX);
        assert_eq!(key[1], IDX_CONFIG);
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
