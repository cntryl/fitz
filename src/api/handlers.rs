//! Transport handlers: TCP and WebSocket

use crate::api::runtime_ingress::Ingress;
use crate::boot::Runtime;
use crate::observability as obs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};

mod http_listener;
mod metrics_listener;
mod tcp_listener;
mod tcp_session;
mod websocket;

pub use http_listener::{spawn_http_listener, spawn_http_listener_with_bound_socket};
pub use metrics_listener::{spawn_metrics_listener, spawn_metrics_listener_with_bound_socket};
pub use tcp_listener::{spawn_tcp_listener, spawn_tcp_listener_with_bound_socket};

pub struct ListenerHandle {
    pub ready: tokio::sync::oneshot::Receiver<()>,
    pub shutdown: tokio::sync::oneshot::Sender<()>,
    pub join: JoinHandle<()>,
}

pub(super) type SessionTasks = Arc<Mutex<JoinSet<()>>>;

/// Shared ownership for one accepted transport connection.
///
/// HTTP owns the connection until a WebSocket upgrade is handed off. After
/// handoff, the WebSocket task owns the same lease. The atomic finalizer keeps
/// connection counters and the semaphore permit exactly-once even when a task
/// panics or multiple transport paths observe the same close.
pub(super) struct ConnectionLifecycle {
    inner: Arc<ConnectionLifecycleInner>,
    http_owner: bool,
    finalizer_owner: bool,
}

impl Clone for ConnectionLifecycle {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            // Only the accepted-connection owner may finalize the HTTP side.
            // Request and upgrade clones are observers; their Drop must not
            // release the permit while HTTP keep-alive is still active.
            http_owner: false,
            finalizer_owner: false,
        }
    }
}

struct ConnectionLifecycleInner {
    runtime: Arc<Runtime>,
    permit: std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    handed_off: AtomicBool,
    finished: AtomicBool,
}

impl ConnectionLifecycle {
    pub(super) fn new(runtime: Arc<Runtime>, permit: tokio::sync::OwnedSemaphorePermit) -> Self {
        Self {
            inner: Arc::new(ConnectionLifecycleInner {
                runtime,
                permit: std::sync::Mutex::new(Some(permit)),
                handed_off: AtomicBool::new(false),
                finished: AtomicBool::new(false),
            }),
            http_owner: true,
            finalizer_owner: true,
        }
    }

    pub(super) fn websocket_owner(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            http_owner: false,
            finalizer_owner: true,
        }
    }

    pub(super) fn handoff_to_websocket(&self) {
        self.inner.handed_off.store(true, Ordering::Release);
    }

    pub(super) fn finish_http(&self) {
        if !self.inner.handed_off.load(Ordering::Acquire) {
            self.finish();
        }
    }

    pub(super) fn finish(&self) {
        if self
            .inner
            .finished
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            if let Ok(mut permit) = self.inner.permit.lock() {
                drop(permit.take());
            }
            record_connection_closed();
            self.inner.runtime.decrement_connections();
        }
    }
}

impl Drop for ConnectionLifecycle {
    fn drop(&mut self) {
        if (self.http_owner && !self.inner.handed_off.load(Ordering::Acquire))
            || (!self.http_owner && self.finalizer_owner)
        {
            self.finish();
        }
    }
}

/// Idempotent session close boundary shared by transport task paths.
pub(super) struct SessionCloseFinalizer {
    ingress: Arc<dyn Ingress>,
    router: Arc<crate::runtime::Router>,
    session_id: u64,
    routes: std::sync::Mutex<Vec<crate::runtime::routing::RouteAddress>>,
    finish_lock: tokio::sync::Mutex<()>,
    finished: AtomicBool,
}

impl SessionCloseFinalizer {
    pub(super) fn new(
        ingress: Arc<dyn Ingress>,
        router: Arc<crate::runtime::Router>,
        session_id: u64,
        initial_route: crate::runtime::routing::RouteAddress,
    ) -> Self {
        Self {
            ingress,
            router,
            session_id,
            routes: std::sync::Mutex::new(vec![initial_route]),
            finish_lock: tokio::sync::Mutex::new(()),
            finished: AtomicBool::new(false),
        }
    }

    pub(super) fn add_route(&self, route: crate::runtime::routing::RouteAddress) {
        if let Ok(mut routes) = self.routes.lock() {
            if !routes.contains(&route) {
                routes.push(route);
            }
        }
    }

    pub(super) async fn finish(&self, reason: crate::session::CloseReason) {
        let _finish_guard = self.finish_lock.lock().await;
        if self.finished.load(Ordering::Acquire) {
            return;
        }

        let routes = self
            .routes
            .lock()
            .map(|routes| routes.clone())
            .unwrap_or_default();
        for route in routes {
            self.router.unregister(&route);
        }
        self.ingress.on_close(self.session_id, reason).await;
        self.finished.store(true, Ordering::Release);
    }
}

pub(super) fn session_tasks() -> SessionTasks {
    Arc::new(Mutex::new(JoinSet::new()))
}

pub(super) async fn reap_session_tasks(label: &'static str, tasks: &SessionTasks) {
    let mut tasks = tasks.lock().await;
    while let Some(result) = tasks.try_join_next() {
        if let Err(error) = result {
            tracing::debug!(listener = label, error = %error, "Session task ended with join error");
        }
    }
}

pub(super) async fn drain_session_tasks(label: &'static str, task_sets: Vec<SessionTasks>) {
    let drain = async {
        for tasks in &task_sets {
            let mut tasks = tasks.lock().await;
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    tracing::debug!(listener = label, error = %error, "Session task ended with join error");
                }
            }
        }
    };

    if tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .is_err()
    {
        tracing::warn!(
            listener = label,
            "Session drain timed out; aborting remaining tasks"
        );
        for tasks in task_sets {
            let mut tasks = tasks.lock().await;
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
    }
}

fn record_connection_opened() {
    if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
        collector.counter_inc(obs::METRIC_CONNECTIONS_OPENED);
    }
}

fn record_connection_closed() {
    if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
        collector.counter_inc(obs::METRIC_CONNECTIONS_CLOSED);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BlockingCleanupSink {
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
        block_once: AtomicBool,
    }

    impl crate::runtime::MailboxSink for BlockingCleanupSink {
        fn deliver(
            &self,
            _envelope: crate::runtime::Envelope,
        ) -> Result<(), crate::runtime::DeliveryError> {
            if self.block_once.swap(false, Ordering::AcqRel) {
                let _ = self.entered.send(());
                let _ = self.release.recv();
            }
            Ok(())
        }

        fn deliver_high_priority(
            &self,
            envelope: crate::runtime::Envelope,
        ) -> Result<(), crate::runtime::DeliveryError> {
            self.deliver(envelope)
        }
    }

    #[tokio::test]
    async fn should_retry_session_finalization_after_cancellation() {
        // Arrange
        let router = Arc::new(crate::runtime::Router::new());
        let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        let blocking_sink = Arc::new(BlockingCleanupSink {
            entered: entered_tx,
            release: release_rx,
            block_once: AtomicBool::new(true),
        });
        for domain in crate::runtime::DomainRegistry::cleanup_order() {
            router.register_domain_pattern(domain.as_str(), blocking_sink.clone());
        }
        let ingress = Arc::new(
            crate::api::runtime_ingress::RuntimeIngress::new(false).with_router(router.clone()),
        );
        let session_id = 7;
        ingress
            .on_open(crate::session::SessionInfo {
                session_id,
                transport_kind: crate::session::TransportKind::WebSocket,
                peer_addr: None,
                metadata: Arc::new(crate::session::SessionMetadata::new()),
                permissions_snapshot: crate::session::SessionPermissions::empty(),
                claims: None,
                authenticated: false,
                route_family: crate::runtime::routing::RouteFamily::new(0),
            })
            .await
            .expect("open session");
        let finalizer = Arc::new(SessionCloseFinalizer::new(
            ingress.clone(),
            router,
            session_id,
            crate::runtime::routing::session_inbox_address(
                crate::runtime::routing::RouteFamily::new(0),
                session_id,
            ),
        ));

        // Act
        let interrupted_finalizer = finalizer.clone();
        let interrupted = tokio::spawn(async move {
            interrupted_finalizer
                .finish(crate::session::CloseReason::ClientClose)
                .await;
        });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(1)))
            .await
            .expect("join cleanup entry wait")
            .expect("cleanup entered blocking sink");
        interrupted.abort();
        release_tx.send(()).expect("release cleanup sink");
        let _ = interrupted.await;
        finalizer
            .finish(crate::session::CloseReason::ClientClose)
            .await;

        // Assert
        assert_eq!(ingress.session_count(), 0);
    }

    #[tokio::test]
    async fn should_finalize_connection_counter_and_permit_exactly_once() {
        // Arrange
        let runtime = Arc::new(Runtime::new(Arc::new(crate::runtime::Router::new())));
        runtime.increment_connections();
        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = limiter.clone().try_acquire_owned().expect("permit");
        let lifecycle = ConnectionLifecycle::new(runtime.clone(), permit);

        // Act
        {
            let _observer = lifecycle.clone();
        }
        lifecycle.finish();
        lifecycle.finish();

        // Assert
        assert_eq!(runtime.connection_count(), 0);
        assert_eq!(limiter.available_permits(), 1);
    }

    #[tokio::test]
    async fn should_transfer_connection_ownership_to_websocket() {
        // Arrange
        let runtime = Arc::new(Runtime::new(Arc::new(crate::runtime::Router::new())));
        runtime.increment_connections();
        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = limiter.clone().try_acquire_owned().expect("permit");
        let lifecycle = ConnectionLifecycle::new(runtime.clone(), permit);
        let observer = lifecycle.clone();
        lifecycle.handoff_to_websocket();
        let websocket_owner = lifecycle.websocket_owner();

        // Act
        drop(observer);
        drop(lifecycle);
        assert_eq!(runtime.connection_count(), 1);
        websocket_owner.finish();

        // Assert
        assert_eq!(runtime.connection_count(), 0);
        assert_eq!(limiter.available_permits(), 1);
    }
}
