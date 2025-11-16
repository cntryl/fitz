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
    decode_lease_info,
    decode_stored_queue_message,
    encode_lease_info,
    encode_stored_queue_message,
};
use crate::core::queue::types::{LeaseInfo, QueueMessage, StoredQueueMessage};
use crate::storage::markers::{queue as queue_prefixes, QUEUE_DOMAIN_PREFIX};
use crate::storage::traits::KvStore;
use lexkey::{encode_composite, Encodable};
use std::sync::Arc;
use uuid::Uuid;

// NOTE: The QueueService currently uses the default column family (0) for all
// queue data. Once Midge exposes richer column-family APIs we can thread those
// through explicitly.

// Default column family for queue operations
const DEFAULT_CF: cntryl_midge::ColumnFamilyId = cntryl_midge::ColumnFamilyId(0);

/// Queue domain prefix marker
const DOMAIN_PREFIX: u8 = QUEUE_DOMAIN_PREFIX;

/// Index type markers (second byte after domain prefix)
const IDX_MESSAGE: u8 = queue_prefixes::MESSAGE;
const IDX_LEASE: u8 = queue_prefixes::LEASE;

/// QueueService owns all queue business logic.
/// Uses KvStore for durable persistence.
/// Leases are persisted as separate rows; no in-memory lease state.
pub struct QueueService {
    kv_store: Arc<dyn KvStore>,
    token_key: Vec<u8>,
}

impl QueueService {
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        // Generate random HMAC key for delivery tokens
        let uuid = Uuid::new_v4();
        let key = uuid.as_bytes().to_vec();

        Self {
            kv_store,
            token_key: key,
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
    pub fn key_lease(realm: &str, area: &str, resource: &str, message_id: &str) -> Vec<u8> {
        encode_composite!(DOMAIN_PREFIX, IDX_LEASE, realm, area, resource, message_id)
            .as_bytes()
            .to_vec()
    }

    /// Build lease prefix (no message id): {DOMAIN_PREFIX} {IDX_LEASE} {realm} {area} {resource}
    pub fn lease_prefix(realm: &str, area: &str, resource: &str) -> Vec<u8> {
        encode_composite!(DOMAIN_PREFIX, IDX_LEASE, realm, area, resource)
            .as_bytes()
            .to_vec()
    }

    /// Compute end key for a given prefix range scan.
    ///
    /// This returns the smallest byte sequence that is greater than all
    /// possible extensions of the given prefix, suitable as an exclusive
    /// upper bound in lexicographic scans.
    pub fn prefix_end(prefix: &[u8]) -> Vec<u8> {
        let mut v = prefix.to_vec();
        v.push(0xFF);
        v
    }

    /// Derive the message key from a lease key by rewriting the index byte.
    pub fn derive_message_key_from_lease(lease_key: &[u8]) -> Option<Vec<u8>> {
        // The lexkey encoding puts each u8 into an 8-byte encoded block.
        // Structure: [DOMAIN_PREFIX encoded (8 bytes)][IDX encoded (8 bytes)][rest...]
        // We need to change the IDX byte which is at a specific position in the second block.
        
        // Minimum length check: need at least 2 encoded blocks (16 bytes)
        if lease_key.len() < 16 {
            return None;
        }
        
        let mut key = lease_key.to_vec();
        
        // Find where IDX_LEASE byte is located
        // Based on the debug output, lexkey encodes u8 values with leading zeros
        // and the actual value at the end of each 8-byte block
        // Let's search for IDX_LEASE in the first 20 bytes and replace it with IDX_MESSAGE
        let mut found = false;
        for i in 8..std::cmp::min(20, key.len()) {
            if key[i] == IDX_LEASE {
                key[i] = IDX_MESSAGE;
                found = true;
                break;
            }
        }
        
        if found {
            Some(key)
        } else {
            None
        }
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
        let message_key = Self::key_message(realm, area, resource, &message_id);

        // Initial lease row: available, zero deliveries
        let initial_lease = LeaseInfo {
            lease_expiry: None,
            lease_owner: None,
            delivery_count: 0,
        };
        let lease_data = encode_lease_info(&initial_lease);
        let lease_key = Self::key_lease(realm, area, resource, &message_id);

        // Write immutable message row then lease row
        self
            .kv_store
            .put(DEFAULT_CF, &message_key, &message_data)
            .map_err(|e| format!("Storage error: {:?}", e))?;

        self
            .kv_store
            .put(DEFAULT_CF, &lease_key, &lease_data)
            .map_err(|e| format!("Storage error: {:?}", e))?;

        Ok(message_id)
    }

    /// Reserve (lease) messages from the specified queue
    pub async fn receive(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        batch_size: usize,
        lease_secs: u32,
    ) -> Result<Vec<QueueMessage>, String> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("Time error: {:?}", e))?
            .as_secs();
        let lease_prefix = Self::lease_prefix(realm, area, resource);
        let end_key = Self::prefix_end(&lease_prefix);

        eprintln!("DEBUG: lease_prefix = {:?}", lease_prefix);
        eprintln!("DEBUG: end_key = {:?}", end_key);

        // Phase 1: scan lease rows to find available messages
        let scan_results = self
            .kv_store
            .scan(DEFAULT_CF, &lease_prefix, end_key.as_slice())
            .map_err(|e| format!("Scan error: {:?}", e))?;

        eprintln!("DEBUG: scan returned {} items", scan_results.len());

        let mut message_keys: Vec<Vec<u8>> = Vec::new();
        let mut staged_leases: Vec<(Vec<u8>, LeaseInfo)> = Vec::new();

        for (lease_key_bytes, lease_bytes) in scan_results {
            eprintln!("DEBUG: found lease key = {:?}", lease_key_bytes);
            if staged_leases.len() >= batch_size {
                break;
            }

            let mut lease_info: LeaseInfo = match decode_lease_info(&lease_bytes) {
                Ok(info) => info,
                Err(e) => {
                    eprintln!("DEBUG: failed to decode lease: {:?}", e);
                    LeaseInfo {
                        lease_expiry: None,
                        lease_owner: None,
                        delivery_count: 0,
                    }
                },
            };

            eprintln!("DEBUG: lease_info = {:?}", lease_info);

            let available = match lease_info.lease_expiry {
                Some(expiry) => {
                    let avail = expiry <= now;
                    eprintln!("DEBUG: has expiry, expired={}, expiry={}, now={}", avail, expiry, now);
                    avail
                },
                None => {
                    let avail = lease_info.lease_owner.is_none();
                    eprintln!("DEBUG: no expiry, owner is none={}", avail);
                    avail
                },
            };

            eprintln!("DEBUG: available = {}", available);

            if !available {
                eprintln!("DEBUG: skipping unavailable lease");
                continue;
            }

            // derive message key
            let message_key = match Self::derive_message_key_from_lease(&lease_key_bytes) {
                Some(k) => {
                    eprintln!("DEBUG: derived message key = {:?}", k);
                    k
                },
                None => {
                    eprintln!("DEBUG: failed to derive message key from lease");
                    continue;
                },
            };

            lease_info.delivery_count = lease_info.delivery_count.saturating_add(1);
            staged_leases.push((lease_key_bytes.to_vec(), lease_info));
            message_keys.push(message_key);
            eprintln!("DEBUG: added to staged_leases and message_keys");
        }

        eprintln!("DEBUG: after scan loop, message_keys.len() = {}", message_keys.len());

        if message_keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let mut lease_updates: Vec<(Vec<u8>, LeaseInfo)> = Vec::new();

        eprintln!("DEBUG: starting phase 2 (hydration)");

        for (message_key, (lease_key, lease_info)) in
            message_keys.into_iter().zip(staged_leases.into_iter())
        {
            eprintln!("DEBUG: hydrating message_key = {:?}", message_key);
            let value_bytes = match self.kv_store.get(DEFAULT_CF, &message_key) {
                Ok(Some(v)) => {
                    eprintln!("DEBUG: found message in KV");
                    v
                },
                Ok(None) => {
                    eprintln!("DEBUG: message not found in KV, cleaning up orphaned lease");
                    // Message missing: clean up orphaned lease
                    self
                        .kv_store
                        .delete(DEFAULT_CF, &lease_key)
                        .map_err(|e| format!("Storage error: {:?}", e))?;
                    continue;
                }
                Err(e) => {
                    eprintln!("DEBUG: KV get error: {:?}", e);
                    return Err(format!("Storage error: {:?}", e));
                },
            };

            let stored: StoredQueueMessage = match decode_stored_queue_message(&value_bytes) {
                Ok(m) => m,
                Err(_) => {
                    // Corrupted message: best-effort cleanup
                    self
                        .kv_store
                        .delete(DEFAULT_CF, &lease_key)
                        .map_err(|e| format!("Storage error: {:?}", e))?;
                    self
                        .kv_store
                        .delete(DEFAULT_CF, &message_key)
                        .map_err(|e| format!("Storage error: {:?}", e))?;
                    continue;
                }
            };

            // TTL enforcement: drop expired messages
            if let Some(ttl) = stored.ttl_secs {
                let expiry_ts = stored.created_at.saturating_add(ttl);
                if expiry_ts <= now {
                    self
                        .kv_store
                        .delete(DEFAULT_CF, &lease_key)
                        .map_err(|e| format!("Storage error: {:?}", e))?;
                    self
                        .kv_store
                        .delete(DEFAULT_CF, &message_key)
                        .map_err(|e| format!("Storage error: {:?}", e))?;
                    continue;
                }
            }

            let lease_expiry = now.saturating_add(lease_secs as u64);
            let token = self.generate_delivery_token(
                realm,
                area,
                resource,
                &stored.id,
                lease_expiry,
                lease_info.delivery_count,
            );

            let lease_info = LeaseInfo {
                lease_expiry: Some(lease_expiry),
                lease_owner: Some(token.clone()),
                delivery_count: lease_info.delivery_count,
            };

            let message = QueueMessage {
                id: stored.id.clone(),
                route: stored.route.clone(),
                body: stored.body.clone(),
                lease_expiry: lease_info.lease_expiry,
                lease_owner: lease_info.lease_owner.clone(),
                delivery_count: lease_info.delivery_count,
                created_at: stored.created_at,
                ttl_secs: stored.ttl_secs,
            };

            results.push(message);
            lease_updates.push((lease_key, lease_info));
        }

        // Phase 3: commit new leases
        for (lease_key, lease_info) in lease_updates {
            let data = encode_lease_info(&lease_info);
            self
                .kv_store
                .put(DEFAULT_CF, &lease_key, &data)
                .map_err(|e| format!("Storage error: {:?}", e))?;
        }

        Ok(results)
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

        // Check lease record for ownership
        let lease_key = Self::key_lease(realm, area, resource, message_id);
        let lease_data = self
            .kv_store
            .get(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?
            .ok_or_else(|| "Lease not found".to_string())?;

        let lease_info: LeaseInfo = decode_lease_info(&lease_data)?;

        // Verify lease ownership
        if lease_info.lease_owner.as_deref() != Some(delivery_token) {
            return Err("Invalid delivery token".to_string());
        }

        // Delete both message and lease records
        let message_key = Self::key_message(realm, area, resource, message_id);
        self
            .kv_store
            .delete(DEFAULT_CF, &message_key)
            .map_err(|e| format!("Storage error: {:?}", e))?;
        self
            .kv_store
            .delete(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?;

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

        // Get lease record
        let lease_key = Self::key_lease(realm, area, resource, message_id);
        let lease_data = self
            .kv_store
            .get(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?
            .ok_or_else(|| "Lease not found".to_string())?;

        let mut lease_info: LeaseInfo = decode_lease_info(&lease_data)?;

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
        self
            .kv_store
            .put(DEFAULT_CF, &lease_key, &updated_data)
            .map_err(|e| format!("Storage error: {:?}", e))?;

        Ok(())
    }

    /// Generate a delivery token for a message
    pub(crate) fn generate_delivery_token(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        message_id: &str,
        lease_expiry: u64,
        delivery_count: u32,
    ) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.token_key).expect("HMAC can take key of any size");
        mac.update(realm.as_bytes());
        mac.update(area.as_bytes());
        mac.update(resource.as_bytes());
        mac.update(message_id.as_bytes());
        mac.update(&lease_expiry.to_be_bytes());
        mac.update(&delivery_count.to_be_bytes());

        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    /// Verify a delivery token
    pub(crate) fn verify_delivery_token(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        message_id: &str,
        token: &str,
    ) -> Result<(), String> {
        // Load current lease and recompute the expected token
        let lease_key = Self::key_lease(realm, area, resource, message_id);
        let lease_data = self
            .kv_store
            .get(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?
            .ok_or_else(|| "Lease not found".to_string())?;

        let lease_info: LeaseInfo = decode_lease_info(&lease_data)?;
        let lease_expiry = lease_info
            .lease_expiry
            .ok_or_else(|| "Message not leased".to_string())?;

        let expected = self.generate_delivery_token(
            realm,
            area,
            resource,
            message_id,
            lease_expiry,
            lease_info.delivery_count,
        );

        if expected == token {
            Ok(())
        } else {
            Err("Invalid or expired token".to_string())
        }
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
        self.verify_delivery_token(realm, area, resource, message_id, delivery_token)?;

        // Get lease record
        let lease_key = Self::key_lease(realm, area, resource, message_id);
        let lease_data = self
            .kv_store
            .get(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?
            .ok_or_else(|| "Lease not found".to_string())?;

        let lease_info: LeaseInfo = decode_lease_info(&lease_data)?;

        // Verify lease ownership
        if lease_info.lease_owner.as_deref() != Some(delivery_token) {
            return Err("Invalid delivery token".to_string());
        }

        // Delete lease record to release lease
        self
            .kv_store
            .delete(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?;

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
        self.verify_delivery_token(realm, area, resource, message_id, delivery_token)?;

        // Get lease record
        let lease_key = Self::key_lease(realm, area, resource, message_id);
        let lease_data = self
            .kv_store
            .get(DEFAULT_CF, &lease_key)
            .map_err(|e| format!("Storage error: {:?}", e))?
            .ok_or_else(|| "Lease not found".to_string())?;

        let lease_info: LeaseInfo = decode_lease_info(&lease_data)?;

        // Verify lease ownership
        if lease_info.lease_owner.as_deref() != Some(delivery_token) {
            return Err("Invalid delivery token".to_string());
        }

        // Reset delivery count and clear lease
        let updated_lease = LeaseInfo {
            lease_expiry: None,
            lease_owner: None,
            delivery_count: 0,
        };

        // Update lease record
        let updated_data = encode_lease_info(&updated_lease);
        self
            .kv_store
            .put(DEFAULT_CF, &lease_key, &updated_data)
            .map_err(|e| format!("Storage error: {:?}", e))?;

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

        // Act & Assert - Receive
        let messages = service
            .receive("test", "realm", "queue", 1, 30)
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

        // Verify message is gone: direct KV lookup
        let key = QueueService::key_message("test", "realm", "queue", &message_id);
        let stored = service
            .kv_store
            .get(DEFAULT_CF, &key)
            .expect("Storage get should succeed");
        assert!(stored.is_none(), "Message should be deleted after completion");
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
            .receive("test", "realm", "queue", 1, 1)
            .await
            .expect("Reserve should succeed");

        assert_eq!(messages.len(), 1);
        let _delivery_token = messages[0].lease_owner.as_ref().unwrap().clone();

        // Simulate lease expiry by clearing lease owner/expiry
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

        // Act - Reserve again (should get the same message)
        let redelivered_messages = service
            .receive("test", "realm", "queue", 1, 30)
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
