//! Stream domain constants

/// Debounce interval for watermark/commit notifications (milliseconds)
pub const NOTICE_DEBOUNCE_MS: u64 = 25;

/// Retry interval for failed watermark persistence attempts (milliseconds)
pub const WATERMARK_PERSIST_RETRY_MS: u64 = 25;

/// Maximum response items accepted from one synchronous Stream read.
pub const MAX_READ_ITEMS: usize = 10_000;

/// Maximum sparse posting entries examined in one synchronous work slice.
pub const MAX_POSTING_ENTRIES_EXAMINED: usize = 4_096;

/// Maximum immutable posting fragments fetched in one synchronous work slice.
pub const MAX_POSTING_FRAGMENTS_FETCHED: usize = 1_024;

/// Maximum live per-scope watermark coordinator actors in each keyed pool.
pub const MAX_WATERMARK_COORDINATORS: usize = 64;

/// Area segment reserved for internal broker coordination (`RealmActor` routing).
///
/// Clients must not use this string as an area name. Any route with this area
/// is rejected at the sink with an explicit error before reaching actor logic.
pub const INTERNAL_REALM_SEGMENT: &str = "__realm__";

/// Resource segment reserved for internal broker coordination (`AreaActor` routing).
///
/// Clients must not use this string as a resource name. Any route with this
/// resource is rejected at the sink with an explicit error before reaching
/// actor logic.
pub const INTERNAL_AREA_SEGMENT: &str = "__area__";
