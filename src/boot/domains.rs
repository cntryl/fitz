//! Domain actor setup and registration

use crate::boot::runtime::BootResult;
use crate::runtime::{DeliveryError, DomainKind, Envelope, MailboxSink, Router};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Arc as StdArc;

#[cfg(test)]
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

pub use crate::domains::kv::sink::KvDomainSink;
pub use crate::domains::lease::sink::LeaseDomainSink;
pub use crate::domains::notice::sink::NoticeDomainSink;
pub use crate::domains::queue::sink::QueueDomainSink;
pub use crate::domains::rpc::sink::RpcDomainSink;
pub use crate::domains::schedule::sink::ScheduleDomainSink;
pub use crate::domains::stream::sink::StreamDomainSink;

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

impl DomainHandles {
    pub fn stop(&self) {
        self.kv.stop();
        self.queue.stop();
        self.notice.stop();
        self.stream.stop();
        self.rpc.stop();
        self.lease.stop();
        self.schedule.stop();
    }
}

pub struct DomainSetupOptions {
    pub server_write_options: cntryl_midge::WriteOptions,
    pub queue_write_options: cntryl_midge::WriteOptions,
    pub queue_fast_flush_interval: Option<std::time::Duration>,
    pub request_sync_write_options: cntryl_midge::WriteOptions,
    pub rpc_request_timeout: Option<std::time::Duration>,
    pub stream_storage_layout: crate::domains::stream::StreamStorageLayout,
}

/// Set up all 7 domain actors and register them with the router.
pub fn setup(
    router: &StdArc<Router>,
    store: &StdArc<cntryl_midge::Engine>,
    admin_read_model: &Arc<crate::control::admin::read_model::AdminReadModel>,
    options: DomainSetupOptions,
) -> BootResult<DomainHandles> {
    let metrics = (*crate::observability::metrics()).clone();
    let storage = crate::storage::FitzStorageEngine::new(store.clone());

    let kv_sink = Arc::new(
        KvDomainSink::new(store.clone(), router.clone(), admin_read_model.clone())
            .with_sync_write_options(options.request_sync_write_options)
            .with_metrics(metrics.clone()),
    );
    DomainKind::Kv
        .descriptor()
        .register_sink(router, kv_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered KV domain (handles kv://* across all route families)");

    let queue_sink = Arc::new(
        QueueDomainSink::try_new_with_storage(
            storage.clone(),
            router.clone(),
            admin_read_model.clone(),
            options.queue_write_options,
            crate::utils::idempotency::default_dedup_store(),
        )?
        .with_fast_flush_interval(options.queue_fast_flush_interval)
        .with_metrics(metrics.clone()),
    );
    DomainKind::Queue
        .descriptor()
        .register_sink(router, queue_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Queue domain (handles queue://* across all route families)");

    let notice_sink = Arc::new(
        NoticeDomainSink::new(router.clone(), admin_read_model.clone())
            .with_metrics(metrics.clone()),
    );
    DomainKind::Notice
        .descriptor()
        .register_sink(router, notice_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Notice domain (handles notice://* across all route families)");

    let stream_sink = Arc::new(
        StreamDomainSink::new_with_storage_layout(
            storage.clone(),
            router.clone(),
            admin_read_model.clone(),
            options.stream_storage_layout,
        )?
        .with_sync_write_options(options.request_sync_write_options)
        .with_metrics(metrics.clone()),
    );
    DomainKind::Stream
        .descriptor()
        .register_sink(router, stream_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered Stream domain (handles stream://* across all route families)");

    let rpc_sink = Arc::new(
        RpcDomainSink::new(router.clone(), admin_read_model.clone())
            .with_request_timeout(
                options
                    .rpc_request_timeout
                    .unwrap_or(std::time::Duration::from_secs(30)),
            )
            .with_metrics(metrics.clone()),
    );
    DomainKind::Rpc
        .descriptor()
        .register_sink(router, rpc_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!("Registered RPC domain (handles rpc://* across all route families)");

    let lease_sink = Arc::new(
        LeaseDomainSink::new(router.clone(), admin_read_model.clone())
            .with_metrics(metrics.clone()),
    );
    DomainKind::Lease
        .descriptor()
        .register_sink(router, lease_sink.clone() as Arc<dyn MailboxSink>);
    tracing::info!(
        "Registered Lease domain (ephemeral, in-memory lease://* across all route families)"
    );

    let schedule_sink = Arc::new(
        ScheduleDomainSink::new_with_storage(storage, router.clone(), admin_read_model.clone())
            .with_write_options(options.server_write_options)
            .with_metrics(metrics.clone()),
    );
    DomainKind::Schedule
        .descriptor()
        .register_sink(router, schedule_sink.clone() as Arc<dyn MailboxSink>);
    schedule_sink
        .preload_persisted_families()
        .map_err(|error| format!("schedule preload failed: {}", error))?;
    tracing::info!("Registered Schedule domain (handles schedule://* across all route families)");

    tracing::info!(
        "All {} domain sinks registered with router",
        DomainKind::ALL.len()
    );

    let handles = DomainHandles {
        kv: kv_sink,
        queue: queue_sink,
        notice: notice_sink,
        stream: stream_sink,
        rpc: rpc_sink,
        lease: lease_sink,
        schedule: schedule_sink,
    };
    crate::api::background::start_domain_background_tasks(&handles);
    Ok(handles)
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
        // Arrange
        let kv_sink = DomainSink::new("kv");
        let notice_sink = DomainSink::new("notice");

        // Act
        let kv_active = kv_sink.active.load(Ordering::Relaxed);
        let notice_active = notice_sink.active.load(Ordering::Relaxed);
        kv_sink.stop();
        let kv_stopped = kv_sink.active.load(Ordering::Relaxed);

        // Assert
        assert!(kv_active);
        assert!(notice_active);
        assert!(!kv_stopped);
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
        // Arrange
        let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

        // Act
        let result = setup(
            &router,
            &store,
            &admin_read_model,
            DomainSetupOptions {
                server_write_options: cntryl_midge::WriteOptions::best_effort(),
                queue_write_options: cntryl_midge::WriteOptions::best_effort(),
                queue_fast_flush_interval: Some(std::time::Duration::from_millis(100)),
                request_sync_write_options: cntryl_midge::WriteOptions::sync(),
                rpc_request_timeout: None,
                stream_storage_layout: crate::domains::stream::StreamStorageLayout::default(),
            },
        );

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_register_all_manifest_domains_for_session_cleanup() {
        // Arrange
        let store = crate::testkit::midge::create_test_engine_with_cfs(vec![1, 2, 3, 4, 5, 6, 7]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

        // Act
        setup(
            &router,
            &store,
            &admin_read_model,
            DomainSetupOptions {
                server_write_options: cntryl_midge::WriteOptions::best_effort(),
                queue_write_options: cntryl_midge::WriteOptions::best_effort(),
                queue_fast_flush_interval: Some(std::time::Duration::from_millis(100)),
                request_sync_write_options: cntryl_midge::WriteOptions::sync(),
                rpc_request_timeout: None,
                stream_storage_layout: crate::domains::stream::StreamStorageLayout::default(),
            },
        )
        .expect("setup domains");

        // Assert
        for domain in DomainKind::ALL {
            let result = router.route(Envelope::new(
                RouteAddress::new(RouteFamily::new(1), domain.cleanup_route()),
                crate::runtime::SessionCleanup { session_id: 42 },
            ));
            assert!(
                result.is_ok(),
                "expected {} cleanup route to be registered",
                domain.as_str()
            );
        }
    }
}
