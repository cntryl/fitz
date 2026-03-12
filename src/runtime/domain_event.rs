// LAYER: RUNTIME
//! Domain-agnostic publish primitive for runtime-routed event delivery
//!
//! Any domain actor can emit a `DomainPublishEvent` via `Context::publish_event()`.
//! The router delivers it to whichever domain sink handles the target route scheme.
//! The receiving sink downcasts the envelope payload to `DomainPublishEvent` and
//! performs subscription matching + fanout internally.
//!
//! # Design
//!
//! `DomainPublishEvent` is intentionally minimal: it carries just enough context
//! for the receiving domain sink to perform subscription matching (route) and
//! build a NOTIFY frame (payload). It does NOT carry domain-specific types,
//! ensuring zero compile-time coupling between domains.
//!
//! # Usage
//!
//! ```text
//! // Inside a Stream actor after commit:
//! let event = DomainPublishEvent::new(family, route, payload);
//! ctx.publish_event(event);  // routes to StreamDomainSink
//!
//! // Inside a Schedule actor after fire:
//! let own = DomainPublishEvent::new(family, schedule_route, payload);
//! ctx.publish_event(own);    // routes to ScheduleDomainSink
//!
//! let exec = DomainPublishEvent::new(family, target_route, payload);
//! ctx.publish_event(exec);   // routes to target domain sink (e.g. NoticeDomainSink)
//! ```

use crate::runtime::routing::{Route, RouteFamily};
use bytes::Bytes;

/// Domain-agnostic publish event for runtime-routed event delivery.
///
/// Emitted by domain actors via `Context::publish_event()` and received
/// by domain sinks via the `Envelope` payload downcast in `MailboxSink::deliver()`.
///
/// The `route` field determines which domain sink receives the event (based on
/// the route scheme). The receiving sink matches the route against its subscription
/// index and fans out NOTIFY frames to matching subscribers.
#[derive(Debug, Clone)]
pub struct DomainPublishEvent {
    /// Route family for isolation boundary
    pub family_id: RouteFamily,
    /// Target route (e.g. `stream://realm/area/resource/committed`)
    pub route: Route,
    /// Opaque payload bytes (domain-specific content, typically JSON)
    pub payload: Bytes,
}

impl DomainPublishEvent {
    /// Create a new domain publish event.
    ///
    /// # Arguments
    ///
    /// * `family_id` - Route family for isolation
    /// * `route` - Target route that determines which domain sink receives this event
    /// * `payload` - Opaque payload bytes carried in the NOTIFY frame
    pub fn new(family_id: RouteFamily, route: Route, payload: Bytes) -> Self {
        Self {
            family_id,
            route,
            payload,
        }
    }
}

/// Session cleanup event sent to domain sinks when a session disconnects.
///
/// Routed to all subscribable domain sinks (Notice, Stream, Schedule) by the
/// session manager's `on_close()` handler. Each domain sink removes all
/// subscriptions associated with the given session_id.
#[derive(Debug, Clone)]
pub struct SessionCleanup {
    /// Session being disconnected
    pub session_id: u64,
}
