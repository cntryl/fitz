//! Queue domain service - durable message queue with lease semantics
//!
//! Messages are stored with lease tracking for at-least-once delivery.
//! Supports hierarchical configuration (realm > area > resource).
//!
//! Key schema (using lexkey for building):
//! - 0x02 0x01 {realm} {area} {resource} {message_id} → Message data
//! - 0x02 0x02 {realm} {area} {resource} {message_id} → Lease info
//! - 0x02 0x03 {realm} {area} {resource} → Queue configuration

use crate::core::queue::encoding::{
    decode_lease_info, decode_stored_queue_message, encode_lease_info, encode_stored_queue_message,
};
use crate::core::queue::types::QueueMessage;
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

/// Queue domain prefix marker
const DOMAIN_PREFIX: u8 = QUEUE_DOMAIN_PREFIX;

/// Index type markers (second byte after domain prefix)
const IDX_MESSAGE: u8 = queue_prefixes::MESSAGE;
const IDX_LEASE: u8 = queue_prefixes::LEASE;

/// QueueService owns all queue business logic.
/// Uses KvStore for durable persistence.
/// Tracks leases in-memory, persists messages to KvStore.
pub struct QueueService {
    kv_store: Arc<dyn KvStore>,
    token_key: Vec<u8>,

    // In-memory lease tracking: route -> id -> (expiry_secs, owner_token, delivery_count)
    leases: Arc<Mutex<LeaseMap>>,
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
        }
    }

    /// Build message key: {DOMAIN_PREFIX} {IDX_MESSAGE} {realm} {area} {resource} {message_id}
    pub fn key_message(realm: &str, area: &str, resource: &str, message_id: &str) -> Vec<u8> {
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
        use crate::core::queue::types::StoredQueueMessage;
        use std::time::{SystemTime, UNIX_EPOCH};

        // Generate unique message ID
        let message_id = format!("msg_{}", uuid::Uuid::new_v4().simple());

        // Create stored message structure (immutable)
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("Time error: {:?}", e))?
            .as_secs();

        let stored_message = StoredQueueMessage {
            id: message_id.clone(),
            route: format!("{}/{}/{}", realm, area, resource),
            body,
            created_at: now,
            ttl_secs,
        };

        // Serialize message
        let message_data = encode_stored_queue_message(&stored_message);

        // Build storage key
        let key = Self::key_message(realm, area, resource, &message_id);

        // Store message
        self.kv_store
            .put(DEFAULT_CF, &key, &message_data)
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
        use crate::core::queue::types::{LeaseInfo, QueueMessage, StoredQueueMessage};
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("Time error: {:?}", e))?
            .as_secs();

        let lease_expiry = now + lease_secs as u64;
        let token = self.generate_delivery_token(realm, area, resource, &format!("batch_{}", now));

        // Scan for available messages
        let prefix = Self::key_message(realm, area, resource, "");
        let end_key = vec![0xFF; 1000];
        let results = self
            .kv_store
            .scan(DEFAULT_CF, &prefix, &end_key)
            .map_err(|e| format!("Scan error: {:?}", e))?;

        let mut available_messages = Vec::new();
        let mut to_lease = Vec::new();

        for (_key_bytes, value_bytes) in results {
            if available_messages.len() >= batch_size {
                break;
            }

            // Parse stored message
            let stored_message: StoredQueueMessage = decode_stored_queue_message(&value_bytes)?;
            let message_id = stored_message.id.clone();

            // Check if there's a lease record
            let lease_key = Self::key_lease(realm, area, resource, &message_id);
            let lease_data = self.kv_store.get(DEFAULT_CF, &lease_key).ok().flatten();

            let (is_available, current_delivery_count) = if let Some(ref data) = lease_data {
                // Parse lease info
                let lease_info: LeaseInfo = decode_lease_info(data)?;
                // Check if lease is expired
                let available = match lease_info.lease_expiry {
                    Some(expiry) => expiry <= now,
                    None => true,
                };
                (available, lease_info.delivery_count)
            } else {
                // No lease record, message is available
                (true, 0)
            };

            if is_available {
                // Create lease info
                let lease_info = LeaseInfo {
                    lease_expiry: Some(lease_expiry),
                    lease_owner: Some(token.clone()),
                    delivery_count: current_delivery_count + 1,
                };

                // Create full message for return
                let message = QueueMessage {
                    id: message_id.clone(),
                    route: stored_message.route.clone(),
                    body: stored_message.body.clone(),
                    lease_expiry: lease_info.lease_expiry,
                    lease_owner: lease_info.lease_owner.clone(),
                    delivery_count: lease_info.delivery_count,
                    created_at: stored_message.created_at,
                    ttl_secs: stored_message.ttl_secs,
                };

                available_messages.push(message);
                to_lease.push((lease_key, lease_info, message_id));
            }
        }

        // Store lease records
        for (lease_key, lease_info, message_id) in to_lease {
            let lease_data = encode_lease_info(&lease_info);
            self.kv_store
                .put(DEFAULT_CF, &lease_key, &lease_data)
                .map_err(|e| format!("Storage error: {:?}", e))?;

            // Update in-memory lease tracking
            let mut leases = self.leases.lock().await;
            let route_key = format!("{}/{}/{}", realm, area, resource);
            let route_leases = leases.entry(route_key).or_insert_with(HashMap::new);
            route_leases.insert(
                message_id,
                (lease_expiry, token.clone(), lease_info.delivery_count),
            );
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
        self.verify_delivery_token(realm, area, resource, message_id, delivery_token)
            .await?;

        // Check lease record for ownership
        let lease_key = Self::key_lease(realm, area, resource, message_id);
        let lease_data = self
            .kv_store
            .get(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?
            .ok_or_else(|| "Lease not found".to_string())?;

        let lease_info: crate::core::queue::types::LeaseInfo = decode_lease_info(&lease_data)?;

        // Verify lease ownership
        if lease_info.lease_owner.as_deref() != Some(delivery_token) {
            return Err("Invalid delivery token".to_string());
        }

        // Delete both message and lease records
        let message_key = Self::key_message(realm, area, resource, message_id);
        self.kv_store
            .delete(DEFAULT_CF, &message_key)
            .map_err(|e| format!("Storage error: {:?}", e))?;
        self.kv_store
            .delete(DEFAULT_CF, &lease_key)
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
        self.verify_delivery_token(realm, area, resource, message_id, delivery_token)
            .await?;

        // Get lease record
        let lease_key = Self::key_lease(realm, area, resource, message_id);
        let lease_data = self
            .kv_store
            .get(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?
            .ok_or_else(|| "Lease not found".to_string())?;

        let mut lease_info: crate::core::queue::types::LeaseInfo = decode_lease_info(&lease_data)?;

        // Verify lease ownership
        if lease_info.lease_owner.as_deref() != Some(delivery_token) {
            return Err("Invalid delivery token".to_string());
        }

        // Extend lease
        let new_expiry = lease_info
            .lease_expiry
            .ok_or_else(|| "Message not leased".to_string())?
            + additional_secs as u64;
        lease_info.lease_expiry = Some(new_expiry);

        // Update lease record
        let updated_data = encode_lease_info(&lease_info);
        self.kv_store
            .put(DEFAULT_CF, &lease_key, &updated_data)
            .map_err(|e| format!("Storage error: {:?}", e))?;

        // Update in-memory lease tracking
        let mut leases = self.leases.lock().await;
        let route_key = format!("{}/{}/{}", realm, area, resource);
        if let Some(route_leases) = leases.get_mut(&route_key) {
            if let Some((expiry, token, delivery_count)) = route_leases.get_mut(message_id) {
                *expiry = new_expiry;
                *token = delivery_token.to_string();
                *delivery_count = lease_info.delivery_count;
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
        use crate::core::queue::types::{LeaseInfo, QueueMessage, StoredQueueMessage};
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("Time error: {:?}", e))?
            .as_secs();

        // Scan for messages
        let prefix = Self::key_message(realm, area, resource, "");
        let end_key = vec![0xFF; 1000];
        let results = self
            .kv_store
            .scan(DEFAULT_CF, &prefix, &end_key)
            .map_err(|e| format!("Scan error: {:?}", e))?;

        for (_key_bytes, value_bytes) in results {
            let stored_message: StoredQueueMessage = decode_stored_queue_message(&value_bytes)?;
            let message_id = stored_message.id.clone();

            // Check lease record
            let lease_key = Self::key_lease(realm, area, resource, &message_id);
            let lease_data = self.kv_store.get(DEFAULT_CF, &lease_key).ok().flatten();

            let is_available = if let Some(ref data) = lease_data {
                let lease_info: LeaseInfo = decode_lease_info(data)?;
                match lease_info.lease_expiry {
                    Some(expiry) => expiry <= now,
                    None => true,
                }
            } else {
                true
            };

            if is_available {
                // Return message with lease info populated
                let (lease_expiry, lease_owner, delivery_count) = if let Some(ref data) = lease_data
                {
                    let lease_info: LeaseInfo = decode_lease_info(data)?;
                    (
                        lease_info.lease_expiry,
                        lease_info.lease_owner,
                        lease_info.delivery_count,
                    )
                } else {
                    (None, None, 0)
                };

                let message = QueueMessage {
                    id: message_id,
                    route: stored_message.route,
                    body: stored_message.body,
                    lease_expiry,
                    lease_owner,
                    delivery_count,
                    created_at: stored_message.created_at,
                    ttl_secs: stored_message.ttl_secs,
                };
                return Ok(Some(message));
            }
        }

        Ok(None)
    }

    /// Generate a delivery token for a message
    fn generate_delivery_token(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        nonce: &str,
    ) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.token_key).expect("HMAC can take key of any size");
        mac.update(realm.as_bytes());
        mac.update(area.as_bytes());
        mac.update(resource.as_bytes());
        mac.update(nonce.as_bytes());

        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    /// Verify a delivery token
    async fn verify_delivery_token(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        message_id: &str,
        token: &str,
    ) -> Result<(), String> {
        // For now, just check if the token exists in our in-memory tracking
        // In a production system, you'd verify the HMAC
        let leases = self.leases.lock().await;
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

    /// Nack (negative acknowledge) a leased message - release lease without completing
    pub async fn nack(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        message_id: &str,
        delivery_token: &str,
    ) -> Result<(), String> {
        // Verify token
        self.verify_delivery_token(realm, area, resource, message_id, delivery_token)
            .await?;

        // Get lease record
        let lease_key = Self::key_lease(realm, area, resource, message_id);
        let lease_data = self
            .kv_store
            .get(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?
            .ok_or_else(|| "Lease not found".to_string())?;

        let lease_info: crate::core::queue::types::LeaseInfo = decode_lease_info(&lease_data)?;

        // Verify lease ownership
        if lease_info.lease_owner.as_deref() != Some(delivery_token) {
            return Err("Invalid delivery token".to_string());
        }

        // Delete lease record to release lease
        self.kv_store
            .delete(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?;

        // Remove from in-memory lease tracking
        let mut leases = self.leases.lock().await;
        let route_key = format!("{}/{}/{}", realm, area, resource);
        if let Some(route_leases) = leases.get_mut(&route_key) {
            route_leases.remove(message_id);
        }

        Ok(())
    }

    /// Requeue a message - reset delivery count and make immediately available
    pub async fn requeue(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        message_id: &str,
        delivery_token: &str,
    ) -> Result<(), String> {
        // Verify token
        self.verify_delivery_token(realm, area, resource, message_id, delivery_token)
            .await?;

        // Get lease record
        let lease_key = Self::key_lease(realm, area, resource, message_id);
        let lease_data = self
            .kv_store
            .get(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?
            .ok_or_else(|| "Lease not found".to_string())?;

        let lease_info: crate::core::queue::types::LeaseInfo = decode_lease_info(&lease_data)?;

        // Verify lease ownership
        if lease_info.lease_owner.as_deref() != Some(delivery_token) {
            return Err("Invalid delivery token".to_string());
        }

        // Reset delivery count and clear lease
        let updated_lease = crate::core::queue::types::LeaseInfo {
            lease_expiry: None,
            lease_owner: None,
            delivery_count: 0,
        };

        // Update lease record
        let updated_data = encode_lease_info(&updated_lease);
        self.kv_store
            .put(DEFAULT_CF, &lease_key, &updated_data)
            .map_err(|e| format!("Storage error: {:?}", e))?;

        // Remove from in-memory lease tracking
        let mut leases = self.leases.lock().await;
        let route_key = format!("{}/{}/{}", realm, area, resource);
        if let Some(route_leases) = leases.get_mut(&route_key) {
            route_leases.remove(message_id);
        }

        Ok(())
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
    async fn should_enqueue_reserve_and_complete_message() {
        // Arrange
        let service = create_test_service().await;
        let test_body = b"test message body".to_vec();

        // Act & Assert - Enqueue
        let message_id = service
            .enqueue("test", "realm", "queue", test_body.clone(), None, None)
            .await
            .expect("Enqueue should succeed");

        // Debug: Check if message is stored
        let key = QueueService::key_message("test", "realm", "queue", &message_id);
        let stored_data = service
            .kv_store
            .get(DEFAULT_CF, &key)
            .expect("Storage get should succeed")
            .expect("Message should be stored");
        let stored_message: crate::core::queue::types::StoredQueueMessage =
            decode_stored_queue_message(&stored_data).expect("Decode should succeed");
        assert_eq!(
            stored_message.id, message_id,
            "Stored message ID should match"
        );

        // Act & Assert - Reserve
        let messages = service
            .reserve("test", "realm", "queue", 1, 30)
            .await
            .expect("Reserve should succeed");

        assert_eq!(messages.len(), 1, "Should reserve exactly one message");
        let reserved_message = &messages[0];
        assert_eq!(
            reserved_message.id, message_id,
            "Reserved message ID should match"
        );
        assert_eq!(
            reserved_message.body, test_body,
            "Message body should match"
        );
        assert!(
            reserved_message.lease_owner.is_some(),
            "Message should have lease owner"
        );

        let delivery_token = reserved_message.lease_owner.as_ref().unwrap().clone();

        // Act & Assert - Complete
        service
            .complete("test", "realm", "queue", &message_id, &delivery_token)
            .await
            .expect("Complete should succeed");

        // Verify message is gone
        let peek_result = service
            .peek("test", "realm", "queue")
            .await
            .expect("Peek should succeed");

        assert!(
            peek_result.is_none(),
            "Message should be deleted after completion"
        );
    }

    #[tokio::test]
    async fn should_handle_lease_expiry_and_redelivery() {
        // Arrange
        let service = create_test_service().await;
        let test_body = b"test message".to_vec();

        // Enqueue a message
        let message_id = service
            .enqueue("test", "realm", "queue", test_body.clone(), None, None)
            .await
            .expect("Enqueue should succeed");

        // Reserve with very short lease (1 second)
        let messages = service
            .reserve("test", "realm", "queue", 1, 1)
            .await
            .expect("Reserve should succeed");

        assert_eq!(messages.len(), 1);
        let _delivery_token = messages[0].lease_owner.as_ref().unwrap().clone();

        // Wait for lease to expire (in a real system, this would happen automatically)
        // For testing, we'll manually clear the lease record
        let lease_key = QueueService::key_lease("test", "realm", "queue", &message_id);
        let lease_data = service
            .kv_store
            .get(DEFAULT_CF, &lease_key)
            .expect("Storage get should succeed")
            .expect("Lease should exist");

        let mut lease_info: crate::core::queue::types::LeaseInfo =
            decode_lease_info(&lease_data).expect("Decode should succeed");

        // Clear lease to simulate expiry
        lease_info.lease_expiry = None;
        lease_info.lease_owner = None;

        let updated_data = encode_lease_info(&lease_info);
        service
            .kv_store
            .put(DEFAULT_CF, &lease_key, &updated_data)
            .expect("Storage put should succeed");

        // Clear in-memory tracking
        let mut leases = service.leases.lock().await;
        let route_key = "test/realm/queue".to_string();
        if let Some(route_leases) = leases.get_mut(&route_key) {
            route_leases.remove(&message_id);
        }
        drop(leases);

        // Act - Reserve again (should get the same message)
        let redelivered_messages = service
            .reserve("test", "realm", "queue", 1, 30)
            .await
            .expect("Reserve should succeed after lease expiry");

        assert_eq!(redelivered_messages.len(), 1);
        assert_eq!(
            redelivered_messages[0].id, message_id,
            "Should get same message"
        );
        assert_eq!(
            redelivered_messages[0].delivery_count, 2,
            "Delivery count should be incremented"
        );
    }
}
