//! Stream domain constants

/// Debounce interval for watermark/commit notifications (milliseconds)
pub const NOTICE_DEBOUNCE_MS: u64 = 25;

/// Default lease size for resource-level offsets
pub const DEFAULT_LEASE_SIZE: u64 = 10_000;

/// Default lease block size for realm-level offsets
pub const DEFAULT_REALM_LEASE_BLOCK: u64 = 10_000;

/// Area segment reserved for internal broker coordination (RealmActor routing).
///
/// Clients must not use this string as an area name. Any route with this area
/// is rejected at the sink with an explicit error before reaching actor logic.
pub const INTERNAL_REALM_SEGMENT: &str = "__realm__";
