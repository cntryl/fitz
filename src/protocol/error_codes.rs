//! Authoritative Error Code Allocation for All Fitz Domains
//!
//! See CLIENT_SPEC.md sections for each domain for the official error code allocations.
//! All domains use numeric error codes allocated in 100-block ranges.
//! Error codes are returned in domain-specific response envelopes as [u8 1][u32 code][u32 msg_len][msg].
//!
//! ## Backpressure Signaling (Per CLIENT_SPEC §Flow Control & Backpressure)
//! When a domain's internal queue is full, the broker returns a backpressure error:
//! - RPC: 6003 = ERR_RPC_BACKPRESSURE (client should retry with exponential backoff)
//! - Queue: 4005 = ERR_QUEUE_FULL (client should retry with exponential backoff)
//! - Other domains: connection may close if internal buffers overflow
//! - Notice PUBLISH: silently drops notifications on backpressure (fire-and-forget semantics, no response sent)

/// KV domain error codes (per CLIENT_SPEC KV Domain section)
pub mod kv {
    pub const ERR_TRANSACTION_NOT_FOUND: u16 = 1001;
    pub const ERR_INVALID_MODE: u16 = 1002;
    pub const ERR_KEY_NOT_FOUND: u16 = 1003;
    pub const ERR_ISOLATION_CONFLICT: u16 = 1004;
    pub const ERR_WRITE_IN_READONLY: u16 = 1005;
    pub const ERR_KEY_EXISTS: u16 = 1006;
    pub const ERR_INVALID_ROUTE: u16 = 1007;
    pub const ERR_REALM_MISMATCH: u16 = 1008;
    pub const ERR_BACKEND_ERROR: u16 = 1009;
    pub const ERR_TRANSACTION_ABORTED: u16 = 1010;
    pub const ERR_UNAUTHORIZED: u16 = 1011; // AC-KV-010: Permission denied for KV operation
}

/// Stream domain error codes (per CLIENT_SPEC Stream Domain section)
pub mod stream {
    pub const ERR_CONCURRENCY_CONFLICT: u16 = 2001;
    pub const ERR_OFFSET_TOO_FAR_AHEAD: u16 = 2002;
    pub const ERR_INVALID_READ_BOUND: u16 = 2003;
    pub const ERR_READ_BEYOND_WATERMARK: u16 = 2004;
    pub const ERR_RESOURCE_NOT_FOUND: u16 = 2005;
    pub const ERR_UNAUTHORIZED: u16 = 2009; // AC-STREAM-005: Permission denied for stream operation
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 2010;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 2011;
}

/// Notice domain error codes (per CLIENT_SPEC Notice Domain section)
pub mod notice {
    pub const ERR_INVALID_ROUTE: u16 = 3001;
    pub const ERR_INVALID_PATTERN: u16 = 3002;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 3003;
    pub const ERR_TRANSPORT_CLOSED: u16 = 3004;
    pub const ERR_UNAUTHORIZED: u16 = 3009; // AC-NOTICE-009: Permission denied for notice operation
}

/// Queue domain error codes (per CLIENT_SPEC Queue Domain section)
pub mod queue {
    pub const ERR_INVALID_TOKEN: u16 = 4001; // AC-QUEUE-006: Invalid or wrong lease token
    pub const ERR_LEASE_EXPIRED: u16 = 4002;
    pub const ERR_MESSAGE_NOT_FOUND: u16 = 4003;
    pub const ERR_QUEUE_NOT_FOUND: u16 = 4004;
    pub const ERR_QUEUE_FULL: u16 = 4005; // Backpressure: queue at capacity, client should retry with backoff
    pub const ERR_UNAUTHORIZED: u16 = 4009; // AC-QUEUE-008: Permission denied for queue operation
}

/// Lease domain error codes (per CLIENT_SPEC Lease Domain section)
pub mod lease {
    pub const ERR_LEASE_HELD: u16 = 5001;
    pub const ERR_INVALID_FENCE: u16 = 5002;
    pub const ERR_LEASE_EXPIRED: u16 = 5003;
    pub const ERR_LEASE_NOT_FOUND: u16 = 5004;
    pub const ERR_INVALID_TOKEN: u16 = 5005; // AC-LEASE-009: Invalid or wrong lease token
    pub const ERR_UNAUTHORIZED: u16 = 5009; // AC-LEASE-010: Permission denied for lease operation
}

/// RPC domain error codes (per CLIENT_SPEC RPC Domain section)
pub mod rpc {
    pub const ERR_RPC_TIMEOUT: u16 = 6001;
    pub const ERR_WORKER_NOT_FOUND: u16 = 6002;
    pub const ERR_RPC_BACKPRESSURE: u16 = 6003; // Backpressure: queue at capacity, client should retry with backoff
    pub const ERR_ROUTE_NOT_REGISTERED: u16 = 6004; // AC-RPC-003: No workers registered for route
    pub const ERR_CORRELATION_NOT_FOUND: u16 = 6005;
    pub const ERR_UNAUTHORIZED: u16 = 6009; // AC-RPC-007, AC-RPC-008: Permission denied for RPC operation
}

/// Schedule domain error codes (per CLIENT_SPEC Schedule Domain section)
pub mod schedule {
    pub const ERR_SCHEDULE_NOT_FOUND: u16 = 7001;
    pub const ERR_INVALID_CRON: u16 = 7002; // AC-SCHEDULE-002: Invalid cron expression syntax
    pub const ERR_SCHEDULE_LIMIT: u16 = 7003;
    pub const ERR_PARSE_ERROR: u16 = 7004;
    pub const ERR_INVALID_TARGET: u16 = 7005;
    pub const ERR_INVALID_SUBSCRIPTION_PATTERN: u16 = 7006;
    pub const ERR_SUBSCRIPTION_LIMIT: u16 = 7007;
    pub const ERR_UNAUTHORIZED: u16 = 7009; // AC-SCHEDULE-008: Permission denied for schedule operation
}
