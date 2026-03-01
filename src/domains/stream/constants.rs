//! Stream domain constants

/// Debounce interval for watermark/commit notifications (milliseconds)
pub const NOTICE_DEBOUNCE_MS: u64 = 25;

/// Default lease size for resource-level offsets
pub const DEFAULT_LEASE_SIZE: u64 = 10_000;

/// Default lease block size for realm-level offsets
pub const DEFAULT_REALM_LEASE_BLOCK: u64 = 10_000;
