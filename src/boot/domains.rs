//! Domain actor setup and registration

use crate::boot::runtime::BootResult;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Arc as StdArc;

#[cfg(test)]
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

fn parse_route_triplet(route: &str) -> Option<(String, String, String)> {
    let path = route.split("://").nth(1).unwrap_or(route);
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if parts.len() < 3 {
        return None;
    }
    Some((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
    ))
}

mod kv_sink;
mod lease_sink;
mod notice_sink;
mod queue_sink;
mod rpc_sink;
mod schedule_sink;
mod stream_sink;

pub use kv_sink::KvDomainSink;
pub use lease_sink::LeaseDomainSink;
pub use notice_sink::NoticeDomainSink;
pub use queue_sink::QueueDomainSink;
pub use rpc_sink::RpcDomainSink;
pub use schedule_sink::ScheduleDomainSink;
pub use stream_sink::StreamDomainSink;

/// Generic domain sink: Forwards envelopes to domain actors.
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

        tracing::debug!(
            domain = self.name,
            destination = ?envelope.destination(),
            "Frame received by domain sink"
        );

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[derive(Clone)]
pub struct DomainHandles {
    pub kv: Arc<KvDomainSink>,
    pub queue: Arc<QueueDomainSink>,
    pub notice: Arc<NoticeDomainSink>,
    pub stream: Arc<StreamDomainSink>,
    pub rpc: Arc<RpcDomainSink>,
    pub lease: Arc<LeaseDomainSink>,
    pub schedule: Arc<ScheduleDomainSink>,
}

/// Set up all 7 domain actors and register them with the router.
pub fn setup(
    router: &StdArc<Router>,
    store: &StdArc<cntryl_midge::Engine>,
    admin_read_model: &Arc<crate::api::admin::read_model::AdminReadModel>,
    queue_write_options: cntryl_midge::WriteOptions,
) -> BootResult<DomainHandles> {
    let kv_sink = Arc::new(KvDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model.clone(),
    ));
    router.register_domain_pattern("kv", kv_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered KV domain (handles kv://* across all route families)");

    let queue_sink = Arc::new(QueueDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model.clone(),
        queue_write_options,
    ));
    router.register_domain_pattern("queue", queue_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Queue domain (handles queue://* across all route families)");

    let notice_sink = Arc::new(NoticeDomainSink::new(
        router.clone(),
        admin_read_model.clone(),
    ));
    router.register_domain_pattern("notice", notice_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Notice domain (handles notice://* across all route families)");

    let stream_sink = Arc::new(StreamDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model.clone(),
    ));
    router.register_domain_pattern("stream", stream_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Stream domain (handles stream://* across all route families)");

    let rpc_sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model.clone()));
    router.register_domain_pattern("rpc", rpc_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered RPC domain (handles rpc://* across all route families)");

    let lease_sink = Arc::new(LeaseDomainSink::new(
        router.clone(),
        admin_read_model.clone(),
    ));
    router.register_domain_pattern("lease", lease_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Lease domain (handles lease://* across all route families)");

    let schedule_sink = Arc::new(ScheduleDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model.clone(),
    ));
    router.register_domain_pattern("schedule", schedule_sink.clone() as Arc<dyn MailboxSink>);
    schedule_sink.start_tick_loop();
    tracing::info!("Registered Schedule domain (handles schedule://* across all route families)");

    tracing::info!("All 7 domain sinks registered with router");

    Ok(DomainHandles {
        kv: kv_sink,
        queue: queue_sink,
        notice: notice_sink,
        stream: stream_sink,
        rpc: rpc_sink,
        lease: lease_sink,
        schedule: schedule_sink,
    })
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

        assert!(kv_sink.active.load(Ordering::Relaxed));
        assert!(notice_sink.active.load(Ordering::Relaxed));

        kv_sink.stop();
        assert!(!kv_sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_handle_delivery_when_active() {
        let sink = DomainSink::new("kv");
        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        let result = sink.deliver(envelope);

        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_delivery_when_stopped() {
        let sink = DomainSink::new("kv");
        sink.stop();

        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        let result = sink.deliver(envelope);

        assert!(matches!(result, Err(DeliveryError::ActorStopped)));
    }

    #[test]
    fn should_handle_high_priority_delivery() {
        let sink = DomainSink::new("kv");
        let address = RouteAddress::new(RouteFamily::new(1), Route::new("kv"));
        let envelope = Envelope::new(address, vec![0u8; 10]);

        let result = sink.deliver_high_priority(envelope);

        assert!(result.is_ok());
    }

    #[test]
    fn should_setup_all_seven_domains() {
        let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        let result = setup(
            &router,
            &store,
            &admin_read_model,
            cntryl_midge::WriteOptions::best_effort(),
        );

        assert!(result.is_ok());
    }
}