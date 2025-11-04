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
