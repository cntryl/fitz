// LAYER: RUNTIME
//! Message routing and delivery infrastructure
//!
//! This module defines the routing layer that sits between message producers
//! and actor mailboxes. The router uses a trait-based sink interface to avoid
//! circular dependencies between the transport and runtime layers.
//!
//! # Architecture
//!
//! ```text
//! [Actor/Client]
//!       ↓
//!   Envelope
//!       ↓
//!    Router  ──→  MailboxSink (trait)
//!                      ↑
//!                      │ implements
//!                      │
//!                  Mailbox (runtime)
//! ```
//!
//! # Design Principles
//!
//! 1. **Narrow Interface**: Router depends only on `MailboxSink` trait, not concrete types
//! 2. **No Runtime Dependency**: Transport layer never imports runtime types
//! 3. **Best-Effort Delivery**: Router does not guarantee delivery or retry
//! 4. **In-Process Only**: No network transparency (future work)
//!
//! # Invariants
//!
//! - Routing is synchronous and fails fast
//! - Unknown destinations are dropped (logged at debug level)
//! - Sink implementations handle backpressure
//! - Router is thread-safe and cloneable
//! - Route families are strictly isolated (no cross-family routing)

use crate::observability as obs;
use crate::runtime::envelope::Envelope;
use crate::runtime::routing::RouteAddress;
use dashmap::DashMap;
use fxhash::FxBuildHasher;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, trace, warn};

fn usize_to_f64_saturating(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Trait for delivering envelopes to actor mailboxes
///
/// This trait provides a narrow interface between the routing layer
/// and the execution layer (runtime). It must be object-safe to allow
/// dynamic dispatch and avoid circular dependencies.
///
/// # Implementation Notes
///
/// Implementers should:
/// - Handle backpressure appropriately (drop, block, or return error)
/// - Be thread-safe (Send + Sync)
/// - Fail fast rather than retry
pub trait MailboxSink: Send + Sync {
    /// Attempt to deliver an envelope to this mailbox
    ///
    /// Returns `Ok(())` if the envelope was accepted, or an error describing
    /// why delivery failed (full, disconnected, etc.).
    ///
    /// # Errors
    ///
    /// Returns `DeliveryError` if:
    /// - Mailbox is at capacity (backpressure)
    /// - Mailbox receiver has been dropped (actor stopped)
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError>;

    /// Attempt to deliver an envelope to the high-priority lane
    ///
    /// **Runtime-internal use only**. This method is used for messages the
    /// runtime or a domain sink explicitly marks as control-plane work, such as
    /// cleanup, live-count reads, admin snapshot refreshes, and test-only panic
    /// probes. Timer callbacks and supervision decisions are handled inside the
    /// managed actor worker unless a concrete domain routes a message here.
    ///
    /// # Errors
    ///
    /// Returns `DeliveryError::HighLaneFull` if the high-priority lane is full.
    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError>;
}

/// Errors that can occur during envelope delivery
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryError {
    /// Mailbox is at capacity (backpressure)
    /// Includes capacity and current length for adaptive backoff
    MailboxFull { capacity: usize, current_len: usize },
    /// High-priority lane is at capacity
    /// This should be rare and indicates the control plane is saturated
    HighLaneFull { capacity: usize, current_len: usize },
    /// Mailbox receiver has been dropped (actor stopped)
    ActorStopped,
    /// A sink panicked while accepting an envelope.
    SinkPanicked,
}

impl DeliveryError {
    /// Get occupancy ratio (0.0 to 1.0) for backpressure decisions
    #[must_use]
    pub fn occupancy(&self) -> f64 {
        match self {
            DeliveryError::MailboxFull {
                capacity,
                current_len,
            }
            | DeliveryError::HighLaneFull {
                capacity,
                current_len,
            } => usize_to_f64_saturating(*current_len) / usize_to_f64_saturating(*capacity),
            DeliveryError::ActorStopped | DeliveryError::SinkPanicked => 1.0,
        }
    }
}

impl std::fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeliveryError::MailboxFull {
                capacity,
                current_len,
            } => {
                write!(f, "Mailbox is full ({current_len}/{capacity} messages)")
            }
            DeliveryError::HighLaneFull {
                capacity,
                current_len,
            } => {
                write!(
                    f,
                    "High-priority lane is full ({current_len}/{capacity} messages)"
                )
            }
            DeliveryError::ActorStopped => write!(f, "Actor has stopped"),
            DeliveryError::SinkPanicked => write!(f, "Sink panicked during delivery"),
        }
    }
}

impl std::error::Error for DeliveryError {}

/// Errors that can occur during routing
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteError {
    /// Destination route not found in registry
    RouteNotFound(RouteAddress),
    /// Delivery failed (mailbox full or actor stopped)
    DeliveryFailed(RouteAddress, DeliveryError),
}

impl std::fmt::Display for RouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteError::RouteNotFound(addr) => write!(f, "Route not found: {addr}"),
            RouteError::DeliveryFailed(addr, err) => {
                write!(f, "Delivery failed for route {addr}: {err}")
            }
        }
    }
}

impl std::error::Error for RouteError {}

/// Fast domain extraction from route string (no allocation)
#[inline]
fn extract_domain(route_str: &str) -> Option<&str> {
    let bytes = route_str.as_bytes();
    for i in 0..bytes.len().saturating_sub(2) {
        if bytes[i] == b':' && bytes[i + 1] == b'/' && bytes[i + 2] == b'/' {
            return Some(&route_str[..i]);
        }
    }
    None
}

/// Route registry mapping `RouteAddress` to mailbox sinks
///
/// Uses `DashMap` for lock-free concurrent access with minimal contention.
/// Enforces route family isolation: routes in different families
/// never conflict even if they have the same path string.
struct RouteRegistry {
    sinks: DashMap<RouteAddress, Arc<dyn MailboxSink>, FxBuildHasher>,
    /// Domain pattern registry: Maps domain prefix (e.g., "kv", "queue") to sink
    /// Used as fallback when exact `RouteAddress` lookup fails
    domain_patterns: DashMap<String, Arc<dyn MailboxSink>, FxBuildHasher>,
}

impl RouteRegistry {
    fn new() -> Self {
        Self {
            // Router lookups and registrations are hot paths. Use a faster
            // non-cryptographic hasher and reserve a modest working set up front
            // to reduce repeated rehashing during actor registration bursts.
            sinks: DashMap::with_capacity_and_hasher(1024, FxBuildHasher::default()),
            domain_patterns: DashMap::with_capacity_and_hasher(32, FxBuildHasher::default()),
        }
    }

    fn register(&self, address: RouteAddress, sink: Arc<dyn MailboxSink>) {
        self.sinks.insert(address, sink);
    }

    fn register_domain_pattern(&self, domain: String, sink: Arc<dyn MailboxSink>) {
        self.domain_patterns.insert(domain, sink);
    }

    fn unregister(&self, address: &RouteAddress) {
        self.sinks.remove(address);
    }

    fn get(&self, address: &RouteAddress) -> Option<Arc<dyn MailboxSink>> {
        self.sinks.get(address).map(|entry| Arc::clone(&*entry))
    }

    fn get_by_domain(&self, domain: &str) -> Option<Arc<dyn MailboxSink>> {
        self.domain_patterns
            .get(domain)
            .map(|entry| Arc::clone(&*entry))
    }

    fn len(&self) -> usize {
        self.sinks.len()
    }

    fn clear(&self) {
        self.sinks.clear();
        self.domain_patterns.clear();
    }
}

/// Message router for in-process delivery
///
/// The router maintains a registry of route-to-mailbox mappings and delivers
/// envelopes to their destinations. It provides best-effort delivery
/// without guarantees or retries.
///
/// # Route Family Isolation
///
/// **CRITICAL**: The router enforces strict isolation between route families.
/// Routes with the same path in different families are completely independent.
///
/// # Thread Safety
///
/// Router is `Clone` and all clones share the same registry. This allows
/// multiple threads to route messages concurrently.
#[derive(Clone)]
pub struct Router {
    registry: Arc<RouteRegistry>,
}

#[derive(Clone, Copy)]
enum MissingRouteKind {
    ExactOrDomainPattern,
    KnownDomainPattern,
}

impl Router {
    fn route_span(dest: &RouteAddress, domain: &str) -> Option<tracing::Span> {
        if should_sample_hot_path() {
            Some(tracing::debug_span!(
                obs::SPAN_ROUTE_MATCH,
                route = %dest,
                domain = domain,
            ))
        } else {
            None
        }
    }

    fn route_match_started_at() -> Option<Instant> {
        obs::hot_path_metrics_enabled().then(Instant::now)
    }

    fn record_route_match_latency(started_at: Option<Instant>) {
        if let Some(start) = started_at {
            crate::observability::histogram_observe_us(
                obs::METRIC_ROUTE_MATCH_LATENCY,
                u128_to_u64_saturating(start.elapsed().as_micros()),
            );
        }
    }

    fn record_delivery_failure(error: &DeliveryError) {
        if let Ok(metrics) = std::panic::catch_unwind(crate::observability::metrics) {
            metrics.counter_inc(obs::METRIC_DELIVERY_FAILURES);
            match error {
                DeliveryError::MailboxFull { .. } => {
                    metrics.counter_inc(obs::METRIC_ROUTER_BACKPRESSURE);
                }
                DeliveryError::HighLaneFull { .. } => {
                    metrics.counter_inc(obs::METRIC_ROUTER_HIGH_LANE_BACKPRESSURE);
                }
                DeliveryError::ActorStopped | DeliveryError::SinkPanicked => {}
            }
        }
    }

    fn catch_sink_panic(
        delivery: impl FnOnce() -> Result<(), DeliveryError>,
    ) -> Result<(), DeliveryError> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(delivery))
            .unwrap_or(Err(DeliveryError::SinkPanicked))
    }

    fn record_route_mismatch() {
        if let Ok(metrics) = std::panic::catch_unwind(crate::observability::metrics) {
            metrics.counter_inc(obs::METRIC_ROUTE_MISMATCHES);
        }
    }

    fn route_not_found(
        dest: &RouteAddress,
        domain: &str,
        missing_route_kind: MissingRouteKind,
    ) -> RouteError {
        match missing_route_kind {
            MissingRouteKind::ExactOrDomainPattern => {
                warn!(destination = %dest, domain = domain, "Router: no route found (exact or domain pattern)");
            }
            MissingRouteKind::KnownDomainPattern => {
                warn!(destination = %dest, domain = domain, "Router: no domain sink found");
            }
        }

        Self::record_route_mismatch();
        RouteError::RouteNotFound(dest.clone())
    }

    fn deliver_with_sink(
        dest: RouteAddress,
        sink: &Arc<dyn MailboxSink>,
        envelope: Envelope,
        started_at: Option<Instant>,
    ) -> Result<(), RouteError> {
        match Self::catch_sink_panic(|| sink.deliver(envelope)) {
            Ok(()) => {
                debug!(destination = %dest, "Router: envelope delivered successfully");

                Self::record_route_match_latency(started_at);

                Ok(())
            }
            Err(e) => {
                warn!(destination = %dest, error = %e, "Router: delivery failed");

                Self::record_delivery_failure(&e);

                Err(RouteError::DeliveryFailed(dest, e))
            }
        }
    }

    fn route_with_resolved_sink(
        envelope: Envelope,
        domain: &str,
        missing_route_kind: MissingRouteKind,
        sink: Option<Arc<dyn MailboxSink>>,
    ) -> Result<(), RouteError> {
        let dest = envelope.destination().clone();
        let started_at = Self::route_match_started_at();

        trace!(destination = %dest, domain = domain, "Router: routing envelope");

        let route_span = Self::route_span(&dest, domain);
        let _route_guard = route_span.as_ref().map(|span| span.enter());

        let sink = sink.ok_or_else(|| Self::route_not_found(&dest, domain, missing_route_kind))?;

        Self::deliver_with_sink(dest, &sink, envelope, started_at)
    }

    fn resolve_sink_for_route(
        &self,
        dest: &RouteAddress,
        fallback_domain: &str,
    ) -> Option<Arc<dyn MailboxSink>> {
        if let Some(sink) = self.registry.get(dest) {
            trace!(destination = %dest, "Router: exact route match found");
            Some(sink)
        } else {
            trace!(destination = %dest, domain = fallback_domain, "Router: trying domain pattern fallback");
            self.registry.get_by_domain(fallback_domain)
        }
    }

    /// Create a new router with an empty registry
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: Arc::new(RouteRegistry::new()),
        }
    }

    /// Register a route's mailbox sink
    ///
    /// After registration, envelopes addressed to this route will be
    /// delivered to the provided sink.
    ///
    /// # Route Family Isolation
    ///
    /// Routes in different families are independent. Registering
    /// `(family=1, route="/user")` does not affect `(family=2, route="/user")`.
    ///
    /// # Note
    ///
    /// If a route is already registered, the old sink is replaced.
    pub fn register(&self, address: RouteAddress, sink: Arc<dyn MailboxSink>) {
        debug!(route = %address, "Router: registering exact route");
        self.registry.register(address, sink);
    }

    /// Resolve a sink for an exact route address.
    ///
    /// This is intended for domain-local fast paths that can safely bypass
    /// an extra registry lookup in `route()` while preserving delivery behavior.
    #[must_use]
    pub fn resolve_sink(&self, address: &RouteAddress) -> Option<Arc<dyn MailboxSink>> {
        self.registry.get(address)
    }

    /// Resolve a sink for a registered domain pattern.
    ///
    /// This is intended for hot paths that already know the destination domain
    /// and want to avoid an exact-route miss before falling back to domain
    /// pattern matching.
    #[must_use]
    pub fn resolve_domain_sink(&self, domain: &str) -> Option<Arc<dyn MailboxSink>> {
        self.registry.get_by_domain(domain)
    }

    /// Register a domain pattern (e.g., "kv", "queue", "notice")
    ///
    /// Domain patterns are used as a fallback when exact `RouteAddress` lookup fails.
    /// This allows domains to handle all routes matching a prefix across ALL route families.
    ///
    /// # Multi-Tenant Support
    ///
    /// Domain patterns enable multi-tenant routing:
    /// - Realm "acme" (family 1) sends `kv://acme/app/users` → `kv_domain_sink`
    /// - Realm "xyz" (family 2) sends `kv://xyz/app/users` → same `kv_domain_sink`
    ///
    /// The domain sink receives the full `RouteAddress` (family + route) and can
    /// enforce tenant isolation internally.
    pub fn register_domain_pattern(&self, domain: &str, sink: Arc<dyn MailboxSink>) {
        debug!(domain = domain, "Router: registering domain pattern");
        self.registry
            .register_domain_pattern(domain.to_string(), sink);
    }

    pub fn clear(&self) {
        self.registry.clear();
    }

    /// Unregister a route
    ///
    /// After unregistration, envelopes addressed to this route will
    /// fail with `RouteNotFound` error.
    pub fn unregister(&self, address: &RouteAddress) {
        self.registry.unregister(address);
    }

    /// Route an envelope to its destination
    ///
    /// Extracts the destination from the envelope, looks up the registered
    /// sink, and attempts delivery.
    ///
    /// # Routing Strategy
    ///
    /// 1. **Exact Match**: First tries exact `RouteAddress` lookup
    /// 2. **Domain Pattern**: Falls back to domain prefix matching (e.g., "kv://" → kv domain)
    ///
    /// # Route Family Isolation
    ///
    /// **CRITICAL**: No cross-family routing. If an envelope is addressed to
    /// `(family=1, route="/user")`, the router will never attempt delivery to
    /// `(family=2, route="/user")`, even if family 1's route is not registered.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - `RouteNotFound` if the destination is not registered
    /// - `DeliveryFailed` if the sink rejected the envelope
    ///
    /// # Invariants
    ///
    /// - Routing is synchronous and non-blocking (fails fast)
    /// - No retries or queuing on failure
    /// - Deadlines in envelope are not enforced (sink's responsibility)
    pub fn route(&self, envelope: Envelope) -> Result<(), RouteError> {
        let dest = envelope.destination().clone();
        let started_at = Self::route_match_started_at();

        trace!(destination = %dest, "Router: routing envelope");

        let (sink, route_span) = {
            let route_str = envelope.destination().route().as_str();
            let extracted_domain = extract_domain(route_str);
            let domain = extracted_domain.unwrap_or("unknown");
            let fallback_domain = extracted_domain.unwrap_or("");
            let route_span = Self::route_span(&dest, domain);
            let sink = self
                .resolve_sink_for_route(envelope.destination(), fallback_domain)
                .ok_or_else(|| {
                    Self::route_not_found(&dest, domain, MissingRouteKind::ExactOrDomainPattern)
                })?;
            (sink, route_span)
        };
        let _route_guard = route_span.as_ref().map(|span| span.enter());

        Self::deliver_with_sink(dest, &sink, envelope, started_at)
    }

    /// Route an envelope directly via a known domain pattern.
    ///
    /// This avoids an exact-route lookup miss when the caller already resolved
    /// the domain from another signal such as message type.
    ///
    /// # Errors
    ///
    /// Returns `RouteError` when no domain sink is registered or the sink
    /// rejects the envelope.
    pub fn route_to_domain(&self, domain: &str, envelope: Envelope) -> Result<(), RouteError> {
        let sink = self.registry.get_by_domain(domain);

        Self::route_with_resolved_sink(envelope, domain, MissingRouteKind::KnownDomainPattern, sink)
    }

    /// Route an envelope to the high-priority lane (runtime-internal use only)
    ///
    /// **CRITICAL**: This method is for runtime-internal use only and should
    /// never be exposed to user code. It is used for envelopes explicitly marked
    /// as runtime or domain control-plane messages. Timer callbacks and actor
    /// supervision are not router-routed unless a concrete domain sends an
    /// envelope through this path.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - `RouteNotFound` if the destination is not registered
    /// - `DeliveryFailed(HighLaneFull)` if the high-priority lane is full
    ///
    /// # Invariants
    ///
    /// - High-priority lane has the SAME capacity as normal lane (no extra buffer)
    /// - Caller must handle `HighLaneFull` as a critical error (control plane saturated)
    /// - Managed actors read high-priority envelopes before normal data messages
    pub(crate) fn route_high_priority(&self, envelope: Envelope) -> Result<(), RouteError> {
        let dest = envelope.destination().clone();

        let sink = self
            .registry
            .get(&dest)
            .ok_or_else(|| RouteError::RouteNotFound(dest.clone()))?;

        match Self::catch_sink_panic(|| sink.deliver_high_priority(envelope)) {
            Ok(()) => Ok(()),
            Err(e) => {
                warn!(destination = %dest, error = %e, "Router: high-priority delivery failed");
                Self::record_delivery_failure(&e);
                Err(RouteError::DeliveryFailed(dest, e))
            }
        }
    }

    /// Check if a route is registered
    #[must_use]
    pub fn contains(&self, address: &RouteAddress) -> bool {
        self.registry.get(address).is_some()
    }

    /// Get the number of registered routes
    #[must_use]
    pub fn len(&self) -> usize {
        self.registry.len()
    }

    /// Check if the registry is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper: Determine if current hot path operation should be sampled
/// Returns true ~1 in 1000 times (0.1% sampling ratio)
fn should_sample_hot_path() -> bool {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .is_ok_and(|d| u128_to_u64_saturating(d.as_nanos()).is_multiple_of(1000))
}

#[cfg(test)]
mod tests;
