//! Lease domain: ephemeral in-memory coordination
//!
//! Leases are a single-broker coordination primitive with TTL-based ownership
//! and fencing tokens for ordering inside one running process. They are not a
//! durable or distributed lease service, and state is expected to disappear on
//! broker restart or session disconnect.
//!
//! # Contract
//!
//! - **Exclusive ownership**: one owner per lease within the running broker
//! - **Fencing tokens**: process-local monotonic tokens used only while the
//!   broker instance is alive
//! - **TTL-based expiration**: leases expire when their local TTL elapses
//! - **Explicit lease watches**: clients can subscribe to `LEASE_NOTIFY (409)`
//!   for concrete lease routes without polling
//! - **Session cleanup**: disconnect removes session-owned lease state
//! - **No recovery**: restart-loss is expected and must remain visible in tests
//!
//! # Route Format
//!
//! Leases use hierarchical routes: `lease://{realm}/{area}/{resource}`
//!
//! Example: `lease://acme/locks/db-migration`
//!
//! Lease identity is `(family_id, realm, area, resource)` extracted from the route.
//!
//! # Fencing Token Scope
//!
//! Each acquisition returns a token that is only meaningful inside the current
//! broker process. A restart resets the token lineage, so clients must not treat
//! tokens as cluster-wide or durable identifiers.

pub mod metrics;
pub mod protocol;
pub(crate) mod sink;

pub use metrics::LeaseMetrics;
pub use protocol::{
    LeaseClientFrame, LeaseClientNotification, LeaseClientRequest, LeaseClientResponse,
    LeaseMessage, LeaseResponse, LeaseSubscriptionMessage,
};

/// Canonicalize a Lease route for authorization.
///
/// Lease carries two route shapes that must NOT canonicalize the same
/// way:
///
/// - **Concrete routes** (`ACQUIRE`/`EXTEND`/`RELEASE`/`QUERY`) never
///   contain a wildcard segment; they must be exactly three non-empty
///   segments, matching `LeaseKey::from_route_str`'s own exact-only
///   parsing. Routing them through the truncating (non-exact) triplet
///   parser would let an over-long route authorize under a silently
///   shortened identity that the sink then rejects outright.
///
/// - **Selectors** (`SUBSCRIBE`/`UNSUBSCRIBE`/`LIST`) accept the shared
///   depth-three `*`/`**` grammar (routing-design.md §4), which lets a
///   selector resolve to fewer than three raw segments (`lease://**`) or
///   more than three when `**` collapses several (`lease://x/**/y/z`).
///   The generic (non-exact) triplet parser requires at least three raw
///   segments to succeed at all and truncates anything past the third,
///   so it would reject the short forms and silently narrow the long
///   ones to a different selector than the one the sink actually
///   matches against — authorizing a pattern the sink never sees.
///   Selectors are therefore scheme-qualified without truncation;
///   `compile_registration_pattern` (called separately by
///   `extract_auth_route_for_domain`) performs the actual shape and
///   depth validation against the same grammar the sink uses, so
///   authorization and the sink cannot accept different pattern
///   languages.
///
/// Splitting on the presence of a wildcard (mirroring
/// the Stream `canonical_auth_route`) is safe because exact-route Lease
/// operations never carry one: the wire parser rejects any `*`/`**`
/// segment for `ACQUIRE`/`EXTEND`/`RELEASE`/`QUERY` before this is ever
/// reached.
pub(crate) fn canonical_auth_route(route: &str) -> Result<std::borrow::Cow<'_, str>, String> {
    if route.contains('*') {
        return Ok(crate::runtime::auth_route::scheme_prefixed_route(
            "lease", route,
        ));
    }

    crate::runtime::auth_route::canonical_triplet_route("lease", route, true)
}
