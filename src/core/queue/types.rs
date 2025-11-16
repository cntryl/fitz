//! Queue domain types

/// A queue message with metadata for lease tracking, delivery counting, and TTL.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueueMessage {
    pub id: String,
    pub route: String,
    pub body: Vec<u8>,
    /// Optional lease expiry as epoch seconds. None means not reserved.
    pub lease_expiry: Option<u64>,
    /// Which consumer currently holds the lease (delivery token).
    pub lease_owner: Option<String>,
    /// Number of times this record has been delivered (reserved).
    pub delivery_count: u32,
    /// Creation time (epoch seconds) for TTL calculations.
    pub created_at: u64,
    /// Per-message TTL in seconds. None means no per-message TTL.
    pub ttl_secs: Option<u64>,
}

/// Stored version of queue message (immutable, without lease info)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredQueueMessage {
    pub id: String,
    pub route: String,
    pub body: Vec<u8>,
    pub created_at: u64,
    pub ttl_secs: Option<u64>,
}

/// Lease information for a message (mutable metadata)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LeaseInfo {
    pub lease_expiry: Option<u64>,
    pub lease_owner: Option<String>,
    pub delivery_count: u32,
}

/// Queue configuration controlling DLQ, visibility, and TTL policies.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct QueueConfig {
    pub dlq_threshold: u32,
    pub default_visibility_secs: u32, // Default lease duration when not specified
    pub ttl_secs: u64,                // 0 means no TTL expiry
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            dlq_threshold: 5,
            default_visibility_secs: 30,
            ttl_secs: 0,
        }
    }
}

/// Hierarchical scope for queue configuration (realm > area > resource).
#[derive(Debug, Clone)]
pub enum QueueScope {
    Realm {
        realm: String,
    },
    Area {
        realm: String,
        area: String,
    },
    Resource {
        realm: String,
        area: String,
        resource: String,
    },
}

/// Queue statistics snapshot.
#[derive(Debug, Clone)]
pub struct QueueStats {
    pub in_flight_count: u32,
}

/// Queue operation types based on route operation segment.
#[derive(Debug, Clone)]
pub enum QueueOperation {
    /// Enqueue - add message to queue
    Enqueue,
    /// Reserve - reserve messages for processing
    Reserve,
    /// List - list queues or messages
    List,
    /// Consume - acknowledge and remove message
    Consume,
    /// ExtendLease - extend lease on reserved message
    ExtendLease,
    /// Config - get or set queue configuration
    Config,
    /// Nack - negative acknowledge, release lease
    Nack,
    /// Requeue - requeue message
    Requeue,
    /// Get - get message by ID
    Get,
    /// Subscribe - subscribe to queue notifications
    Subscribe,
    /// Unsubscribe - unsubscribe from queue notifications
    Unsubscribe,
}

impl QueueOperation {
    /// Determine operation from route
    pub fn from_route(route: &crate::protocol::route::Route) -> Result<Self, String> {
        match route.operation.as_deref() {
            Some("enqueue") => Ok(QueueOperation::Enqueue),
            Some("reserve") | Some("receive") => Ok(QueueOperation::Reserve),
            Some("list") => Ok(QueueOperation::List),
            Some("consume") | Some("ack") => Ok(QueueOperation::Consume),
            Some("extend-lease") | Some("extend") => Ok(QueueOperation::ExtendLease),
            Some("config") => Ok(QueueOperation::Config),
            Some("nack") => Ok(QueueOperation::Nack),
            Some("requeue") => Ok(QueueOperation::Requeue),
            Some("get") => Ok(QueueOperation::Get),
            Some("subscribe") => Ok(QueueOperation::Subscribe),
            Some("unsubscribe") => Ok(QueueOperation::Unsubscribe),
            None => Err("Queue operation required".to_string()),
            Some(op) => Err(format!("Unknown queue operation: {}", op)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_default_queue_config() {
        // Arrange
        // No setup needed for default

        // Act
        let config = QueueConfig::default();

        // Assert
        assert_eq!(config.dlq_threshold, 5);
        assert_eq!(config.default_visibility_secs, 30);
        assert_eq!(config.ttl_secs, 0);
    }

    #[test]
    fn should_create_queue_message_with_all_fields() {
        // Arrange
        let id = "test_msg_123".to_string();
        let route = "realm/area/resource".to_string();
        let body = b"test message body".to_vec();
        let lease_expiry = Some(1234567890);
        let lease_owner = Some("token_abc".to_string());
        let delivery_count = 2;
        let created_at = 1234567800;
        let ttl_secs = Some(3600);

        // Act
        let message = QueueMessage {
            id: id.clone(),
            route: route.clone(),
            body: body.clone(),
            lease_expiry,
            lease_owner: lease_owner.clone(),
            delivery_count,
            created_at,
            ttl_secs,
        };

        // Assert
        assert_eq!(message.id, id);
        assert_eq!(message.route, route);
        assert_eq!(message.body, body);
        assert_eq!(message.lease_expiry, lease_expiry);
        assert_eq!(message.lease_owner, lease_owner);
        assert_eq!(message.delivery_count, delivery_count);
        assert_eq!(message.created_at, created_at);
        assert_eq!(message.ttl_secs, ttl_secs);
    }

    #[test]
    fn should_create_queue_message_with_minimal_fields() {
        // Arrange
        let id = "minimal_msg".to_string();
        let route = "realm/area/resource".to_string();
        let body = b"body".to_vec();

        // Act
        let message = QueueMessage {
            id: id.clone(),
            route: route.clone(),
            body: body.clone(),
            lease_expiry: None,
            lease_owner: None,
            delivery_count: 0,
            created_at: 1000000000,
            ttl_secs: None,
        };

        // Assert
        assert_eq!(message.id, id);
        assert_eq!(message.route, route);
        assert_eq!(message.body, body);
        assert!(message.lease_expiry.is_none());
        assert!(message.lease_owner.is_none());
        assert_eq!(message.delivery_count, 0);
        assert_eq!(message.created_at, 1000000000);
        assert!(message.ttl_secs.is_none());
    }

    #[test]
    fn should_create_queue_stats() {
        // Arrange
        let in_flight_count = 42;

        // Act
        let stats = QueueStats { in_flight_count };

        // Assert
        assert_eq!(stats.in_flight_count, in_flight_count);
    }

    #[test]
    fn should_create_realm_scope() {
        // Arrange
        let realm = "test_realm".to_string();

        // Act
        let scope = QueueScope::Realm {
            realm: realm.clone(),
        };

        // Assert
        match scope {
            QueueScope::Realm { realm: r } => assert_eq!(r, realm),
            _ => panic!("Expected Realm scope"),
        }
    }

    #[test]
    fn should_create_area_scope() {
        // Arrange
        let realm = "test_realm".to_string();
        let area = "test_area".to_string();

        // Act
        let scope = QueueScope::Area {
            realm: realm.clone(),
            area: area.clone(),
        };

        // Assert
        match scope {
            QueueScope::Area { realm: r, area: a } => {
                assert_eq!(r, realm);
                assert_eq!(a, area);
            }
            _ => panic!("Expected Area scope"),
        }
    }

    #[test]
    fn should_create_resource_scope() {
        // Arrange
        let realm = "test_realm".to_string();
        let area = "test_area".to_string();
        let resource = "test_resource".to_string();

        // Act
        let scope = QueueScope::Resource {
            realm: realm.clone(),
            area: area.clone(),
            resource: resource.clone(),
        };

        // Assert
        match scope {
            QueueScope::Resource {
                realm: r,
                area: a,
                resource: res,
            } => {
                assert_eq!(r, realm);
                assert_eq!(a, area);
                assert_eq!(res, resource);
            }
            _ => panic!("Expected Resource scope"),
        }
    }
}
