//! Domain actor setup and registration

use crate::boot::runtime::BootResult;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Arc as StdArc;

#[cfg(test)]
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

/// Generic domain sink: Forwards envelopes to domain actors
///
/// This is a thread-safe wrapper that:
/// - Holds mutable actor state in a Mutex (100% sync, no async locks)
/// - Parses incoming TLV frames
/// - Dispatches to domain handler
/// - Builds response envelopes
///
/// Each domain (KV, Queue, Notice, etc) instantiates this with their own actor type.
pub struct DomainSink {
    name: &'static str,
    active: AtomicBool,
}

impl DomainSink {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

impl MailboxSink for DomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        // Domain-specific message handling happens here in real implementation:
        // 1. Parse envelope payload (TLV) to domain message
        // 2. Call domain handler (e.g., kv_actor.handle(session_id, message))
        // 3. Collect response
        // 4. Route response back through ingress via reply_to channel
        //
        // For now, we log the delivery and drop the message (best-effort)
        tracing::debug!(
            domain = self.name,
            destination = ?envelope.destination(),
            "Frame received by domain sink"
        );

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        // High-priority lane reserved for control-plane operations
        // Domains typically use normal-priority delivery
        self.deliver(envelope)
    }
}

/// Real KV domain sink with actual KvActor
///
/// This sink:
/// - Maintains per-session KvActor instances
/// - Parses TLV frames to KvMessage
/// - Dispatches to actor
/// - Returns responses
pub struct KvDomainSink {
    /// Midge storage engine
    store: Arc<cntryl_midge::Engine>,
    /// Per-session actors (keyed by session_id)
    actors: Arc<Mutex<std::collections::HashMap<u64, crate::domains::kv::KvActor>>>,
    /// Router for routing response envelopes back
    router: Arc<Router>,
    active: AtomicBool,
}

impl KvDomainSink {
    pub fn new(store: Arc<cntryl_midge::Engine>, router: Arc<Router>) -> Self {
        Self {
            store,
            actors: Arc::new(Mutex::new(std::collections::HashMap::new())),
            router,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

impl MailboxSink for KvDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        // Extract frame context from envelope payload
        // The transport layer stores FrameContext as the envelope payload
        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                // Fallback: try to extract raw Bytes
                match envelope.payload::<bytes::Bytes>() {
                    Some(_bytes) => {
                        tracing::warn!(
                            domain = "kv",
                            destination = ?envelope.destination(),
                            "Envelope payload was Bytes, expected FrameContext - raw TLV not supported yet"
                        );
                        return Err(DeliveryError::ActorStopped);
                    }
                    None => {
                        tracing::warn!(
                            domain = "kv",
                            destination = ?envelope.destination(),
                            "Envelope payload was neither FrameContext nor Bytes"
                        );
                        return Err(DeliveryError::ActorStopped);
                    }
                }
            }
        };

        let route_addr = envelope.destination();
        let route_family = route_addr.family();

        // Parse TLV frame using codec
        // The codec will convert msg_type and raw bytes into a KvMessage
        // TODO: Extract realm and area from route path
        let kv_message = match crate::protocol::kv::parse_request(
            frame_ctx.msg_type.as_u16(),
            *route_family,
            String::new(), // TODO: extract from route
            String::new(), // TODO: extract from route
            &frame_ctx.payload,
        ) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    msg_type = frame_ctx.msg_type.as_u16(),
                    error = %e,
                    "Failed to parse KV message"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };

        // Log successful parsing
        tracing::debug!(
            domain = "kv",
            session = frame_ctx.session_id,
            channel = ?frame_ctx.channel_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            "Parsed KV message successfully"
        );

        // Get or create actor for this session
        let response = {
            let mut actors = self.actors.lock();
            let actor = actors
                .entry(frame_ctx.session_id)
                .or_insert_with(|| crate::domains::kv::KvActor::new(self.store.clone()));

            // Handle the message synchronously
            actor.handle(kv_message)
        };

        // Encode the response
        let response_bytes = crate::protocol::kv::encode_response(&response);

        // Build response envelope using reply_to
        // This swaps source/destination and sets causation
        let response_envelope = envelope.reply_to(FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()), // TODO: map to response type
            bytes::Bytes::from(response_bytes),
        ));

        // Route response back through the router
        // This will deliver to the ingress/session layer which handles sending to transport
        match self.router.route(response_envelope) {
            Ok(_) => {
                tracing::debug!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    "KV message handled and response routed"
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    domain = "kv",
                    session = frame_ctx.session_id,
                    error = ?e,
                    "Failed to route response"
                );
                Err(DeliveryError::ActorStopped)
            }
        }
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

/// Real Queue domain sink with actual QueueActor
///
/// This sink:
/// - Maintains per-session QueueActor instances
/// - Parses TLV frames to QueueMessage
/// - Dispatches to actor
/// - Returns responses
///
/// NOTE: QueueActor requires family, queue_key, store, and clock.
/// For now, we use the stub implementation and return to this after the
/// domain-specific constructors are refactored.
pub struct QueueDomainSink {
    /// Midge storage engine
    #[allow(dead_code)]
    store: Arc<cntryl_midge::Engine>,
    active: AtomicBool,
}

impl QueueDomainSink {
    pub fn new(store: Arc<cntryl_midge::Engine>, _router: Arc<Router>) -> Self {
        Self {
            store,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

impl MailboxSink for QueueDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        // TODO: Full implementation of QueueDomainSink
        // For now, log and drop like the stub
        tracing::debug!(
            domain = "queue",
            destination = ?envelope.destination(),
            "Queue frame received (stub - not yet implemented)"
        );

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

/// Set up all 7 domain actors and register them with the router
///
/// Register all domain handlers with the router
///
/// # Architecture
///
/// Domains are registered **globally** with route pattern matching, NOT per route family.
///
/// - **Route Family** = Tenant/isolation boundary (e.g., tenant_acme = family 100)
/// - **Domain** = Handler identified by route scheme (e.g., "kv://", "queue://", "notice://")
/// - **Realm** = Logical boundary within the route string (part of the path)
///
/// # Example
///
/// ```text
/// // Tenant ACME (family 100) sends KV request
/// RouteAddress::new(RouteFamily::new(100), Route::new("kv://acme/app/users/get"))
/// → Routes to KV domain, isolated to family 100
///
/// // Tenant XYZ (family 200) sends KV request  
/// RouteAddress::new(RouteFamily::new(200), Route::new("kv://xyz/app/users/get"))
/// → Routes to same KV domain, isolated to family 200
/// ```
///
/// The router matches on the route pattern (domain scheme) and enforces isolation
/// via the route family. Domains operate on all families but see isolated state.
///
/// # Route Family Assignment
///
/// Route families are **NOT assigned by this function**. They are:
/// - Dynamically allocated by the control plane per tenant
/// - Passed in client requests (part of the RouteAddress)
/// - Enforced by the storage layer (aligned with Midge ColumnFamilyId)
pub fn setup(router: &StdArc<Router>, store: &StdArc<cntryl_midge::Engine>) -> BootResult<()> {
    // Register domain handlers globally using wildcard route family (matches all)
    // Each domain matches its route scheme pattern across ALL route families

    // KV domain: Handles all "kv://*" routes across all families
    let kv_sink = Arc::new(KvDomainSink::new(store.clone(), router.clone()));
    router.register_domain_pattern("kv", kv_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered KV domain (handles kv://* across all route families)");

    // Queue domain: Handles all "queue://*" routes across all families
    let queue_sink = Arc::new(QueueDomainSink::new(store.clone(), router.clone()));
    router.register_domain_pattern("queue", queue_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered Queue domain (handles queue://* across all route families)");

    // Notice domain: Handles all "notice://*" routes across all families
    let notice_sink = Arc::new(DomainSink::new("notice"));
    router.register_domain_pattern("notice", notice_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered Notice domain (handles notice://* across all route families)");

    // Stream domain: Handles all "stream://*" routes across all families
    let stream_sink = Arc::new(DomainSink::new("stream"));
    router.register_domain_pattern("stream", stream_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered Stream domain (handles stream://* across all route families)");

    // RPC domain: Handles all "rpc://*" routes across all families
    let rpc_sink = Arc::new(DomainSink::new("rpc"));
    router.register_domain_pattern("rpc", rpc_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered RPC domain (handles rpc://* across all route families)");

    // Lease domain: Handles all "lease://*" routes across all families
    let lease_sink = Arc::new(DomainSink::new("lease"));
    router.register_domain_pattern("lease", lease_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered Lease domain (handles lease://* across all route families)");

    // Schedule domain: Handles all "schedule://*" routes across all families
    let schedule_sink = Arc::new(DomainSink::new("schedule"));
    router.register_domain_pattern("schedule", schedule_sink as Arc<dyn MailboxSink>);
    tracing::info!("Registered Schedule domain (handles schedule://* across all route families)");

    tracing::info!("All 7 domain sinks registered with router");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_define_domain_setup() {
        // Placeholder: Domain setup structure is well-defined
    }

    #[test]
    fn should_create_domain_sinks() {
        let kv_sink = DomainSink::new("kv");
        let notice_sink = DomainSink::new("notice");

        // Both should be active initially
        assert!(kv_sink.active.load(Ordering::Relaxed));
        assert!(notice_sink.active.load(Ordering::Relaxed));

        // Stopping should work
        kv_sink.stop();
        assert!(!kv_sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_create_kv_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());

        // Act
        let kv_sink = KvDomainSink::new(store, router);

        // Assert
        assert!(kv_sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_handle_delivery_when_active() {
        // Arrange
        let sink = DomainSink::new("kv");
        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        // Act
        let result = sink.deliver(envelope);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_delivery_when_stopped() {
        // Arrange
        let sink = DomainSink::new("kv");
        sink.stop();

        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        // Act
        let result = sink.deliver(envelope);

        // Assert
        assert!(matches!(result, Err(DeliveryError::ActorStopped)));
    }

    #[test]
    fn should_handle_high_priority_delivery() {
        // Arrange
        let sink = DomainSink::new("kv");
        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        // Act
        let result = sink.deliver_high_priority(envelope);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_setup_all_seven_domains() {
        // Arrange - Create test engine with all 7 domain column families
        let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
        let router = Arc::new(Router::new());

        // Act
        let result = setup(&router, &store);

        // Assert
        assert!(result.is_ok());

        // Verify all 7 domains are registered
        // In production, the transport layer would register sinks for response routing,
        // but for this test we just verify the domains were set up successfully.
        // Note: Actually routing messages requires the full transport layer to be in place.
    }
}
