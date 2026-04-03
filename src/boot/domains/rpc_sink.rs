use crate::protocol::frame_context::FrameContext;
use crate::runtime::routing::{
    route_quad, session_inbox_address, Route, RouteAddress, RouteFamily,
};
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use chrono::Utc;
use fxhash::FxBuildHasher;
use parking_lot::Mutex;
use std::cmp::Ordering as HeapOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

type RpcFastMap<K, V> = HashMap<K, V, FxBuildHasher>;

const RPC_BACKPRESSURE_ERROR: &str = "RPC backpressure: too many pending requests";
const RPC_NO_WORKERS_ERROR: &str = "No workers registered for route";
const RPC_WORKER_NOT_FOUND_ERROR: &str = "Worker disconnected or unregistered";
const RPC_CORRELATION_NOT_FOUND_ERROR: &str = "Correlation ID not found (orphaned response)";
const RPC_INVALID_SEQUENCE_ERROR: &str =
    "RPC response sequence must start at seq=0 and advance contiguously";
const RPC_TIMEOUT_ERROR: &str = "Worker did not reply within timeout period";
// RPC remains broker-local and in-memory. Worker registrations and pending
// requests disappear on disconnect cleanup or broker restart; this bound keeps
// that live coordination state from growing without limit in one process.
const RPC_MAX_PENDING_REQUESTS: usize = 4096;
// Admin endpoints read from a coalesced in-memory snapshot so hot-path request
// dispatch does not rewrite the current-process read model on every mutation.
const RPC_ADMIN_SNAPSHOT_INTERVAL_US: u64 = 250_000;
const RPC_DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const RPC_MIN_TIMEOUT_SWEEP_INTERVAL: Duration = Duration::from_millis(10);
const RPC_MAX_TIMEOUT_SWEEP_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Eq, PartialEq)]
struct ExpiringPendingRequest {
    expires_at: Instant,
    correlation_id: uuid::Uuid,
}

impl Ord for ExpiringPendingRequest {
    fn cmp(&self, other: &Self) -> HeapOrdering {
        other
            .expires_at
            .cmp(&self.expires_at)
            .then_with(|| self.correlation_id.cmp(&other.correlation_id))
    }
}

impl PartialOrd for ExpiringPendingRequest {
    fn partial_cmp(&self, other: &Self) -> Option<HeapOrdering> {
        Some(self.cmp(other))
    }
}

fn rpc_timeout_sweep_interval(request_timeout: Duration) -> Duration {
    request_timeout
        .checked_div(4)
        .unwrap_or(RPC_MIN_TIMEOUT_SWEEP_INTERVAL)
        .max(RPC_MIN_TIMEOUT_SWEEP_INTERVAL)
        .min(RPC_MAX_TIMEOUT_SWEEP_INTERVAL)
}

#[derive(Clone)]
struct RpcWorker {
    addr: RouteAddress,
    inbox_addr: RouteAddress,
    session_id: u64,
    registered_at: String,
    requests_handled: u64,
    total_latency_us: u64,
}

impl RpcWorker {
    fn new(addr: RouteAddress, inbox_addr: RouteAddress, session_id: u64) -> Self {
        Self {
            addr,
            inbox_addr,
            session_id,
            registered_at: Utc::now().to_rfc3339(),
            requests_handled: 0,
            total_latency_us: 0,
        }
    }

    #[cfg(test)]
    fn with_stats(
        addr: RouteAddress,
        inbox_addr: RouteAddress,
        session_id: u64,
        registered_at: impl Into<String>,
        requests_handled: u64,
        total_latency_us: u64,
    ) -> Self {
        Self {
            addr,
            inbox_addr,
            session_id,
            registered_at: registered_at.into(),
            requests_handled,
            total_latency_us,
        }
    }

    fn record_completion(&mut self, latency_us: u64) {
        self.requests_handled = self.requests_handled.saturating_add(1);
        self.total_latency_us = self.total_latency_us.saturating_add(latency_us);
    }

    fn average_latency_ms(&self) -> f64 {
        if self.requests_handled == 0 {
            return 0.0;
        }

        self.total_latency_us as f64 / 1000.0 / self.requests_handled as f64
    }
}

#[derive(Debug, Clone)]
struct RpcPendingRequest {
    route: Route,
    caller_session_id: u64,
    caller_inbox_addr: Option<RouteAddress>,
    worker_addr: RouteAddress,
    worker_session_id: u64,
    next_expected_seq: u64,
    submitted_at: String,
    submitted_at_instant: Instant,
    expires_at: Instant,
}

struct RpcPendingRequestInit {
    route: Route,
    caller_session_id: u64,
    caller_inbox_addr: RouteAddress,
    worker_addr: RouteAddress,
    worker_session_id: u64,
    submitted_at: String,
    submitted_at_instant: Instant,
    expires_at: Instant,
}

impl RpcPendingRequest {
    fn new(init: RpcPendingRequestInit) -> Self {
        let RpcPendingRequestInit {
            route,
            caller_session_id,
            caller_inbox_addr,
            worker_addr,
            worker_session_id,
            submitted_at,
            submitted_at_instant,
            expires_at,
        } = init;

        Self {
            route,
            caller_session_id,
            caller_inbox_addr: Some(caller_inbox_addr),
            worker_addr,
            worker_session_id,
            next_expected_seq: 0,
            submitted_at,
            submitted_at_instant,
            expires_at,
        }
    }

    fn from_dispatch(
        req: &crate::domains::rpc::protocol::RpcRequest,
        caller_session_id: u64,
        caller_inbox_addr: RouteAddress,
        worker_addr: RouteAddress,
        worker_session_id: u64,
        expires_at: Instant,
    ) -> Self {
        let submitted_at_instant = Instant::now();
        Self::new(RpcPendingRequestInit {
            route: req.route.clone(),
            caller_session_id,
            caller_inbox_addr,
            worker_addr,
            worker_session_id,
            submitted_at: Utc::now().to_rfc3339(),
            submitted_at_instant,
            expires_at,
        })
    }

    fn age_seconds(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.submitted_at_instant)
            .as_secs()
    }
}

struct RpcPendingErrorDelivery {
    correlation_id: uuid::Uuid,
    caller_session_id: u64,
    caller_inbox_addr: RouteAddress,
}

struct RpcPendingCleanupResult {
    detached_callers: usize,
    removed_pending: usize,
    pending_len: usize,
    disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
}

struct RpcSessionCleanupResult {
    removed_workers: usize,
    detached_callers: usize,
    removed_pending: usize,
    pending_len: usize,
    disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
}

struct RpcWorkerCleanupResult {
    removed_workers: usize,
    removed_pending: usize,
    pending_len: usize,
    disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
}

struct RpcPendingTimeoutResult {
    removed_pending: usize,
    pending_len: usize,
    timeout_deliveries: Vec<RpcPendingErrorDelivery>,
}

struct RpcRouteState {
    workers: Vec<RpcWorker>,
    rr_index: usize,
}

impl RpcRouteState {
    fn new() -> Self {
        Self {
            workers: Vec::new(),
            rr_index: 0,
        }
    }

    fn register_worker(&mut self, worker: RpcWorker) {
        self.workers.push(worker);
    }

    fn record_completion(&mut self, session_id: u64, latency_us: u64) -> bool {
        if let Some(worker) = self
            .workers
            .iter_mut()
            .find(|worker| worker.session_id == session_id)
        {
            worker.record_completion(latency_us);
            return true;
        }

        false
    }

    fn unregister_worker(&mut self, worker_addr: &RouteAddress, session_id: u64) {
        self.workers
            .retain(|worker| worker.addr != *worker_addr || worker.session_id != session_id);

        if self.workers.is_empty() {
            self.rr_index = 0;
        } else {
            self.rr_index %= self.workers.len();
        }
    }

    fn worker_count(&self) -> usize {
        self.workers.len()
    }

    fn unregister_session(&mut self, session_id: u64) -> usize {
        let before = self.workers.len();
        self.workers
            .retain(|worker| worker.session_id != session_id);

        if self.workers.is_empty() {
            self.rr_index = 0;
        } else {
            self.rr_index %= self.workers.len();
        }

        before.saturating_sub(self.workers.len())
    }

    fn select_worker(&mut self) -> Option<RpcWorker> {
        if self.workers.is_empty() {
            return None;
        }

        let pick = self.rr_index % self.workers.len();
        self.rr_index = self.rr_index.wrapping_add(1);
        Some(self.workers[pick].clone())
    }
}

struct RpcPendingTable {
    pending: RpcFastMap<uuid::Uuid, RpcPendingRequest>,
    expirations: BinaryHeap<ExpiringPendingRequest>,
}

#[derive(Debug)]
enum RpcPendingResponseDisposition {
    Missing,
    Forward {
        pending: RpcPendingRequest,
        pending_len: usize,
        removed_pending: bool,
    },
    InvalidSequence {
        pending: RpcPendingRequest,
        pending_len: usize,
        expected_seq: u64,
    },
}

impl RpcPendingTable {
    fn new() -> Self {
        Self {
            pending: HashMap::with_capacity_and_hasher(256, FxBuildHasher::default()),
            expirations: BinaryHeap::with_capacity(256),
        }
    }

    fn track_pending(&mut self, correlation_id: uuid::Uuid, pending: RpcPendingRequest) -> usize {
        let expires_at = pending.expires_at;
        self.pending.insert(correlation_id, pending);
        self.expirations.push(ExpiringPendingRequest {
            expires_at,
            correlation_id,
        });
        self.pending.len()
    }

    fn pending_for_response(
        &mut self,
        correlation_id: &uuid::Uuid,
        seq: u64,
        stream_end: bool,
    ) -> RpcPendingResponseDisposition {
        let Some(expected_seq) = self
            .pending
            .get(correlation_id)
            .map(|pending| pending.next_expected_seq)
        else {
            return RpcPendingResponseDisposition::Missing;
        };

        if seq != expected_seq {
            let pending = self
                .pending
                .remove(correlation_id)
                .expect("tracked pending request for invalid sequence");
            return RpcPendingResponseDisposition::InvalidSequence {
                pending,
                pending_len: self.pending.len(),
                expected_seq,
            };
        }

        if stream_end {
            let pending = self
                .pending
                .remove(correlation_id)
                .expect("tracked pending request for terminal response");
            return RpcPendingResponseDisposition::Forward {
                pending,
                pending_len: self.pending.len(),
                removed_pending: true,
            };
        }

        let tracked = {
            let pending = self
                .pending
                .get_mut(correlation_id)
                .expect("tracked pending request for non-terminal response");
            let tracked = pending.clone();
            pending.next_expected_seq = pending.next_expected_seq.saturating_add(1);
            tracked
        };
        let pending_len = self.pending.len();

        RpcPendingResponseDisposition::Forward {
            pending: tracked,
            pending_len,
            removed_pending: false,
        }
    }

    fn remove_pending(&mut self, correlation_id: &uuid::Uuid) -> Option<usize> {
        self.pending
            .remove(correlation_id)
            .map(|_| self.pending.len())
    }

    fn cleanup_session(&mut self, session_id: u64) -> RpcPendingCleanupResult {
        let mut detached_callers = 0;
        let mut disconnect_deliveries = Vec::new();
        let before = self.pending.len();

        self.pending.retain(|correlation_id, pending| {
            if pending.worker_session_id == session_id {
                if pending.caller_session_id != session_id {
                    if let Some(caller_inbox_addr) = pending.caller_inbox_addr.clone() {
                        disconnect_deliveries.push(RpcPendingErrorDelivery {
                            correlation_id: correlation_id.to_owned(),
                            caller_session_id: pending.caller_session_id,
                            caller_inbox_addr,
                        });
                    }
                }

                return false;
            }

            if pending.caller_session_id == session_id && pending.caller_inbox_addr.is_some() {
                pending.caller_inbox_addr = None;
                detached_callers += 1;
            }

            true
        });

        let removed_pending = before.saturating_sub(self.pending.len());
        RpcPendingCleanupResult {
            detached_callers,
            removed_pending,
            pending_len: self.pending.len(),
            disconnect_deliveries,
        }
    }

    fn cleanup_worker(
        &mut self,
        worker_addr: &RouteAddress,
        worker_session_id: u64,
    ) -> RpcPendingCleanupResult {
        let mut disconnect_deliveries = Vec::new();
        let before = self.pending.len();

        self.pending.retain(|correlation_id, pending| {
            if pending.worker_session_id == worker_session_id && pending.worker_addr == *worker_addr
            {
                if let Some(caller_inbox_addr) = pending.caller_inbox_addr.clone() {
                    disconnect_deliveries.push(RpcPendingErrorDelivery {
                        correlation_id: correlation_id.to_owned(),
                        caller_session_id: pending.caller_session_id,
                        caller_inbox_addr,
                    });
                }

                return false;
            }

            true
        });

        RpcPendingCleanupResult {
            detached_callers: 0,
            removed_pending: before.saturating_sub(self.pending.len()),
            pending_len: self.pending.len(),
            disconnect_deliveries,
        }
    }

    fn expire_timed_out(&mut self, now: Instant) -> RpcPendingTimeoutResult {
        let mut timeout_deliveries = Vec::new();
        let before = self.pending.len();

        while let Some(expiring) = self.expirations.peek() {
            if expiring.expires_at > now {
                break;
            }

            let expiring = self.expirations.pop().expect("pending expiration entry");
            let Some(pending) = self.pending.get(&expiring.correlation_id).cloned() else {
                continue;
            };

            if pending.expires_at != expiring.expires_at {
                continue;
            }

            let pending = self
                .pending
                .remove(&expiring.correlation_id)
                .expect("tracked pending request");

            if let Some(caller_inbox_addr) = pending.caller_inbox_addr {
                timeout_deliveries.push(RpcPendingErrorDelivery {
                    correlation_id: expiring.correlation_id,
                    caller_session_id: pending.caller_session_id,
                    caller_inbox_addr,
                });
            }
        }

        RpcPendingTimeoutResult {
            removed_pending: before.saturating_sub(self.pending.len()),
            pending_len: self.pending.len(),
            timeout_deliveries,
        }
    }

    fn len(&self) -> usize {
        self.pending.len()
    }
}

struct RpcState {
    routes: RpcFastMap<Route, RpcRouteState>,
    pending: RpcPendingTable,
}

fn rpc_admin_snapshot_due(
    snapshot_dirty: bool,
    force: bool,
    now_elapsed_us: u64,
    last_snapshot_elapsed_us: u64,
) -> bool {
    snapshot_dirty
        && (force
            || now_elapsed_us.saturating_sub(last_snapshot_elapsed_us)
                >= RPC_ADMIN_SNAPSHOT_INTERVAL_US)
}

impl RpcState {
    fn new() -> Self {
        Self {
            routes: HashMap::with_capacity_and_hasher(64, FxBuildHasher::default()),
            pending: RpcPendingTable::new(),
        }
    }

    fn ensure_route_state(&mut self, route: &Route) -> &mut RpcRouteState {
        self.routes
            .entry(route.clone())
            .or_insert_with(RpcRouteState::new)
    }

    fn route_state(&mut self, route: &Route) -> Option<&mut RpcRouteState> {
        self.routes.get_mut(route)
    }

    fn cleanup_session(&mut self, session_id: u64) -> RpcSessionCleanupResult {
        let mut removed_workers = 0;
        self.routes.retain(|_, route_state| {
            removed_workers += route_state.unregister_session(session_id);
            route_state.worker_count() > 0
        });

        let pending_cleanup = self.pending.cleanup_session(session_id);

        RpcSessionCleanupResult {
            removed_workers,
            detached_callers: pending_cleanup.detached_callers,
            removed_pending: pending_cleanup.removed_pending,
            pending_len: pending_cleanup.pending_len,
            disconnect_deliveries: pending_cleanup.disconnect_deliveries,
        }
    }

    fn unregister_worker(
        &mut self,
        worker_addr: &RouteAddress,
        session_id: u64,
    ) -> RpcWorkerCleanupResult {
        let removed_workers = {
            let mut removed = 0;
            let mut remove_route = false;

            if let Some(route_state) = self.routes.get_mut(worker_addr.route()) {
                let before = route_state.worker_count();
                route_state.unregister_worker(worker_addr, session_id);
                removed = before.saturating_sub(route_state.worker_count());
                remove_route = route_state.worker_count() == 0;
            }

            if remove_route {
                self.routes.remove(worker_addr.route());
            }

            removed
        };

        let pending_cleanup = self.pending.cleanup_worker(worker_addr, session_id);

        RpcWorkerCleanupResult {
            removed_workers,
            removed_pending: pending_cleanup.removed_pending,
            pending_len: pending_cleanup.pending_len,
            disconnect_deliveries: pending_cleanup.disconnect_deliveries,
        }
    }

    #[cfg(test)]
    fn route_count(&self) -> usize {
        self.routes.len()
    }
}

pub struct RpcDomainSink {
    state: Mutex<RpcState>,
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
    request_timeout: Duration,
    snapshot_dirty: AtomicBool,
    snapshot_syncing: AtomicBool,
    last_snapshot_elapsed_us: AtomicU64,
    snapshot_epoch: Instant,
    metrics: Option<crate::observability::metrics::MetricsCollector>,
}

impl RpcDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            state: Mutex::new(RpcState::new()),
            router,
            admin_read_model,
            active: AtomicBool::new(true),
            request_timeout: RPC_DEFAULT_REQUEST_TIMEOUT,
            snapshot_dirty: AtomicBool::new(false),
            snapshot_syncing: AtomicBool::new(false),
            last_snapshot_elapsed_us: AtomicU64::new(0),
            snapshot_epoch: Instant::now(),
            metrics: None,
        }
    }

    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = if request_timeout.is_zero() {
            RPC_MIN_TIMEOUT_SWEEP_INTERVAL
        } else {
            request_timeout
        };
        self
    }

    pub fn with_metrics(
        mut self,
        metrics: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    /// Start the best-effort timeout sweep for in-memory pending RPC requests.
    ///
    /// Expired requests are removed from the current process, timeout counters are
    /// updated, and terminal errors are forwarded only if the caller inbox is still
    /// registered. Correlation IDs here only match live in-flight work; there is
    /// no replay, broker-side deduplication, or restart recovery path behind this
    /// loop.
    pub fn start_timeout_loop(self: &Arc<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("RPC timeout loop not started: no Tokio runtime available");
            return;
        };

        let weak = Arc::downgrade(self);
        handle.spawn(async move {
            loop {
                let Some(sink) = weak.upgrade() else {
                    break;
                };
                if !sink.active.load(Ordering::Relaxed) {
                    break;
                }

                tokio::time::sleep(rpc_timeout_sweep_interval(sink.request_timeout)).await;

                let Some(sink) = weak.upgrade() else {
                    break;
                };
                if !sink.active.load(Ordering::Relaxed) {
                    break;
                }

                sink.expire_timed_out_requests();
            }
        });
    }

    fn counter_inc(&self, name: &str) {
        if let Some(ref metrics) = self.metrics {
            metrics.counter_inc(name);
        }
    }

    fn counter_add(&self, name: &str, amount: u64) {
        if let Some(ref metrics) = self.metrics {
            metrics.counter_add(name, amount);
        }
    }

    fn gauge_set(&self, name: &str, value: u64) {
        if let Some(ref metrics) = self.metrics {
            metrics.gauge_set(name, value);
        }
    }

    fn histogram_observe_us(&self, name: &str, value_us: u64) {
        if let Some(ref metrics) = self.metrics {
            metrics.histogram_observe_us(name, value_us);
        }
    }

    fn histogram_observe_elapsed_us(&self, name: &str, start: Instant) {
        self.histogram_observe_us(name, start.elapsed().as_micros() as u64);
    }

    fn expire_timed_out_requests(&self) {
        self.expire_timed_out_requests_at(Instant::now());
    }

    fn expire_timed_out_requests_at(&self, now: Instant) {
        let timeout_result = {
            let mut state = self.state.lock();
            state.pending.expire_timed_out(now)
        };

        if timeout_result.removed_pending == 0 {
            return;
        }

        let timeout_delivery_count = timeout_result.timeout_deliveries.len();
        self.gauge_set("rpc_pending_requests", timeout_result.pending_len as u64);
        self.counter_add(
            "rpc_request_timeouts_total",
            timeout_result.removed_pending as u64,
        );
        self.counter_add(
            "rpc_cleanup_pending_removed_total",
            timeout_result.removed_pending as u64,
        );
        self.schedule_admin_snapshot(false);

        tracing::debug!(
            domain = "rpc",
            removed_pending = timeout_result.removed_pending,
            delivered_timeouts = timeout_delivery_count,
            pending_len = timeout_result.pending_len,
            "RPC request timeout sweep applied"
        );

        self.forward_pending_error_deliveries(
            timeout_result.timeout_deliveries,
            crate::protocol::error_codes::rpc::ERR_RPC_TIMEOUT,
            RPC_TIMEOUT_ERROR,
            "rpc_timeout_errors_forwarded_total",
            "rpc_timeout_errors_dropped_total",
        );
    }

    fn remove_pending_request(&self, correlation_id: &uuid::Uuid) -> usize {
        let pending_len = {
            let mut state = self.state.lock();
            match state.pending.remove_pending(correlation_id) {
                Some(pending_len) => pending_len,
                None => state.pending.len(),
            }
        };

        self.gauge_set("rpc_pending_requests", pending_len as u64);
        pending_len
    }

    fn apply_session_cleanup(&self, session_id: u64) -> RpcSessionCleanupResult {
        let cleanup_result = {
            let mut state = self.state.lock();
            state.cleanup_session(session_id)
        };

        self.gauge_set("rpc_pending_requests", cleanup_result.pending_len as u64);
        if cleanup_result.removed_workers > 0 {
            self.counter_add(
                "rpc_cleanup_workers_removed_total",
                cleanup_result.removed_workers as u64,
            );
        }
        if cleanup_result.detached_callers > 0 {
            self.counter_add(
                "rpc_cleanup_callers_detached_total",
                cleanup_result.detached_callers as u64,
            );
        }
        if cleanup_result.removed_pending > 0 {
            self.counter_add(
                "rpc_cleanup_pending_removed_total",
                cleanup_result.removed_pending as u64,
            );
        }
        if cleanup_result.removed_workers > 0
            || cleanup_result.detached_callers > 0
            || cleanup_result.removed_pending > 0
        {
            self.schedule_admin_snapshot(false);
        }

        tracing::debug!(
            domain = "rpc",
            session_id,
            removed_workers = cleanup_result.removed_workers,
            detached_callers = cleanup_result.detached_callers,
            removed_pending = cleanup_result.removed_pending,
            pending_len = cleanup_result.pending_len,
            "RPC session cleanup applied"
        );

        cleanup_result
    }

    fn apply_worker_unsubscribe(
        &self,
        worker_addr: &RouteAddress,
        session_id: u64,
    ) -> RpcWorkerCleanupResult {
        let cleanup_result = {
            let mut state = self.state.lock();
            state.unregister_worker(worker_addr, session_id)
        };

        self.gauge_set("rpc_pending_requests", cleanup_result.pending_len as u64);
        if cleanup_result.removed_workers > 0 {
            self.counter_add(
                "rpc_cleanup_workers_removed_total",
                cleanup_result.removed_workers as u64,
            );
        }
        if cleanup_result.removed_pending > 0 {
            self.counter_add(
                "rpc_cleanup_pending_removed_total",
                cleanup_result.removed_pending as u64,
            );
        }
        if cleanup_result.removed_workers > 0 || cleanup_result.removed_pending > 0 {
            self.schedule_admin_snapshot(false);
        }

        tracing::debug!(
            domain = "rpc",
            worker = worker_addr.route().as_str(),
            session_id,
            removed_workers = cleanup_result.removed_workers,
            removed_pending = cleanup_result.removed_pending,
            pending_len = cleanup_result.pending_len,
            "RPC worker cleanup applied"
        );

        cleanup_result
    }

    fn forward_pending_error_deliveries(
        &self,
        error_deliveries: Vec<RpcPendingErrorDelivery>,
        error_code: u16,
        error_message: &'static str,
        forwarded_counter: &str,
        dropped_counter: &str,
    ) {
        if error_deliveries.is_empty() {
            return;
        }

        let mut error_body_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(96);
        let mut response_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(128);

        for delivery in error_deliveries {
            let error_body = crate::protocol::rpc_codec::encode_error_body_into(
                error_code,
                error_message,
                &mut error_body_encoder,
            );
            let error_response = crate::domains::rpc::protocol::RpcResponse::single(
                delivery.correlation_id,
                bytes::Bytes::from(error_body),
            );
            let encoded_response = crate::protocol::rpc_codec::encode_response_message_into(
                &error_response,
                &mut response_encoder,
            );
            let response_ctx = FrameContext::new(
                delivery.caller_session_id,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(encoded_response),
                *delivery.caller_inbox_addr.family(),
            );
            let response_envelope = Envelope::new(delivery.caller_inbox_addr, response_ctx);

            if let Err(error) = self.router.route(response_envelope) {
                self.counter_inc(dropped_counter);
                tracing::warn!(
                    domain = "rpc",
                    correlation_id = %delivery.correlation_id,
                    error_code,
                    error = ?error,
                    "Failed to forward RPC terminal error to requester"
                );
            } else {
                self.counter_inc(forwarded_counter);
            }
        }
    }

    fn forward_worker_disconnect_errors(
        &self,
        disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
    ) {
        self.forward_pending_error_deliveries(
            disconnect_deliveries,
            crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
            RPC_WORKER_NOT_FOUND_ERROR,
            "rpc_worker_disconnect_errors_forwarded_total",
            "rpc_worker_disconnect_errors_dropped_total",
        );
    }

    /// Copy a point-in-time view of live in-memory RPC state into the admin read
    /// model for the current broker process only.
    ///
    /// This snapshot is intentionally coalesced and may lag very recent subscribe,
    /// unsubscribe, timeout, or cleanup mutations by up to the current sync
    /// interval. It is an operational view, not a durable recovery log.
    fn sync_admin_snapshot(&self) {
        let state = self.state.lock();
        let snapshot_now = Instant::now();
        let workers = state
            .routes
            .iter()
            .flat_map(|(route, route_state)| {
                route_state
                    .workers
                    .iter()
                    .filter_map(|worker| {
                        route_quad(route.as_str()).map(|parts| {
                            crate::api::admin::RpcWorker::snapshot(
                                worker.session_id,
                                parts.realm,
                                route.as_str(),
                                &worker.registered_at,
                                worker.requests_handled,
                                worker.average_latency_ms(),
                            )
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let pending = state
            .pending
            .pending
            .iter()
            .map(|(correlation_id, pending)| {
                crate::api::admin::RpcPendingRequest::snapshot(
                    correlation_id.to_string(),
                    pending.route.as_str(),
                    &pending.submitted_at,
                    pending.age_seconds(snapshot_now),
                    Some(pending.worker_session_id.to_string()),
                )
            })
            .collect();
        self.admin_read_model.replace_rpc_workers(workers);
        self.admin_read_model.replace_rpc_pending(pending);
    }

    pub fn worker_count(&self) -> usize {
        self.state
            .lock()
            .routes
            .values()
            .map(RpcRouteState::worker_count)
            .sum()
    }

    pub fn pending_request_count(&self) -> usize {
        self.state.lock().pending.len()
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        self.maybe_sync_admin_snapshot(false);
    }

    /// Mark the admin snapshot dirty and opportunistically refresh the coalesced view.
    fn schedule_admin_snapshot(&self, force: bool) {
        self.snapshot_dirty.store(true, Ordering::Relaxed);
        self.maybe_sync_admin_snapshot(force);
    }

    /// Sync the admin snapshot when the snapshot interval elapses or a caller forces it.
    ///
    /// Even forced snapshots are still point-in-time copies of the sink's current
    /// in-memory state, not linearizable reads of concurrent RPC activity.
    fn maybe_sync_admin_snapshot(&self, force: bool) {
        #[cfg(feature = "bench-no-snapshot")]
        {
            let _ = force;
            return;
        }

        #[cfg(not(feature = "bench-no-snapshot"))]
        {
            let now_elapsed_us = self.snapshot_epoch.elapsed().as_micros() as u64;
            let last_snapshot_elapsed_us = self.last_snapshot_elapsed_us.load(Ordering::Relaxed);
            let snapshot_dirty = self.snapshot_dirty.load(Ordering::Relaxed);

            if !rpc_admin_snapshot_due(
                snapshot_dirty,
                force,
                now_elapsed_us,
                last_snapshot_elapsed_us,
            ) {
                return;
            }

            if self
                .snapshot_syncing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                return;
            }

            if !self.snapshot_dirty.swap(false, Ordering::AcqRel) {
                self.snapshot_syncing.store(false, Ordering::Release);
                return;
            }

            let snapshot_start = Instant::now();
            self.sync_admin_snapshot();
            let snapshot_time_us = snapshot_start.elapsed().as_micros() as u64;
            self.last_snapshot_elapsed_us.store(
                self.snapshot_epoch.elapsed().as_micros() as u64,
                Ordering::Relaxed,
            );
            self.snapshot_syncing.store(false, Ordering::Release);
            self.histogram_observe_us("rpc_admin_snapshot_us", snapshot_time_us);
        }
    }
}

impl MailboxSink for RpcDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            let cleanup_result = self.apply_session_cleanup(cleanup.session_id);
            self.forward_worker_disconnect_errors(cleanup_result.disconnect_deliveries);
            return Ok(());
        }

        tracing::debug!(
            domain = "rpc",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "RPC domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "rpc", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };

        tracing::debug!(
            domain = "rpc",
            session = frame_ctx.session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            payload_len = frame_ctx.payload.len(),
            "RPC: parsing request"
        );

        let rpc_msg = match crate::protocol::rpc_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
        ) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(domain = "rpc", error = %e, "Failed to parse RPC message");
                return Err(DeliveryError::ActorStopped);
            }
        };

        use crate::domains::rpc::protocol::RpcMessage;
        use crate::protocol::rpc_codec::RpcResponseMsg;
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);

        let (response, snapshot_policy) = match rpc_msg {
            RpcMessage::Subscribe { worker_addr } => {
                let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
                    session_inbox_address(*envelope.destination().family(), frame_ctx.session_id)
                });
                let mut state = self.state.lock();
                let route_state = state.ensure_route_state(worker_addr.route());
                route_state.register_worker(RpcWorker::new(
                    worker_addr.clone(),
                    worker_inbox_addr,
                    frame_ctx.session_id,
                ));
                tracing::debug!(
                    domain = "rpc",
                    worker = worker_addr.route().as_str(),
                    session = frame_ctx.session_id,
                    "Worker registered"
                );
                (Some(RpcResponseMsg::Ok { data: vec![] }), Some(true))
            }
            RpcMessage::Unsubscribe { worker_addr } => {
                let cleanup_result =
                    self.apply_worker_unsubscribe(&worker_addr, frame_ctx.session_id);
                self.forward_worker_disconnect_errors(cleanup_result.disconnect_deliveries);
                tracing::debug!(
                    domain = "rpc",
                    worker = worker_addr.route().as_str(),
                    session = frame_ctx.session_id,
                    removed_workers = cleanup_result.removed_workers,
                    removed_pending = cleanup_result.removed_pending,
                    "Worker unregistered"
                );
                (Some(RpcResponseMsg::Ok { data: vec![] }), Some(true))
            }
            RpcMessage::Request(req) => {
                self.expire_timed_out_requests();
                self.counter_inc("rpc_requests_total");
                let caller_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
                    session_inbox_address(frame_ctx.route_family, frame_ctx.session_id)
                });

                let state_wait_start = Instant::now();
                let mut state = self.state.lock();
                let state_wait_us = state_wait_start.elapsed().as_micros() as u64;
                let state_hold_start = Instant::now();
                let route_registry_lookup_start = Instant::now();
                let route_registry_lookup_us =
                    route_registry_lookup_start.elapsed().as_micros() as u64;
                let route_exists = state.routes.contains_key(&req.route);
                let mut worker_selection_us = 0_u64;

                if !route_exists {
                    let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                    drop(state);

                    self.histogram_observe_us(
                        "rpc_route_registry_lookup_us",
                        route_registry_lookup_us,
                    );
                    self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
                    self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
                    self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
                    self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
                    self.counter_inc("rpc_requests_rejected_no_worker_total");
                    (
                        Some(RpcResponseMsg::CodeError {
                            code: crate::protocol::error_codes::rpc::ERR_ROUTE_NOT_REGISTERED,
                            message: RPC_NO_WORKERS_ERROR.to_string(),
                        }),
                        None,
                    )
                } else if state.pending.len() >= RPC_MAX_PENDING_REQUESTS {
                    let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                    drop(state);

                    self.histogram_observe_us(
                        "rpc_route_registry_lookup_us",
                        route_registry_lookup_us,
                    );
                    self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
                    self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
                    self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
                    self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
                    self.counter_inc("rpc_requests_rejected_backpressure_total");
                    tracing::warn!(
                        domain = "rpc",
                        correlation_id = %req.correlation_id,
                        route = req.route.as_str(),
                        pending_requests = RPC_MAX_PENDING_REQUESTS,
                        "Rejected request due to RPC pending capacity"
                    );
                    (
                        Some(RpcResponseMsg::CodeError {
                            code: crate::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
                            message: RPC_BACKPRESSURE_ERROR.to_string(),
                        }),
                        None,
                    )
                } else {
                    let worker_selection_start = Instant::now();
                    let selected_worker = state
                        .route_state(&req.route)
                        .and_then(|route_state| route_state.select_worker());
                    worker_selection_us = worker_selection_start.elapsed().as_micros() as u64;

                    if let Some(worker) = selected_worker {
                        let expires_at = Instant::now() + self.request_timeout;
                        let pending_track_start = Instant::now();
                        let pending_len = state.pending.track_pending(
                            req.correlation_id,
                            RpcPendingRequest::from_dispatch(
                                &req,
                                frame_ctx.session_id,
                                caller_inbox_addr,
                                worker.addr.clone(),
                                worker.session_id,
                                expires_at,
                            ),
                        ) as u64;
                        let pending_track_us = pending_track_start.elapsed().as_micros() as u64;
                        let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                        drop(state);

                        self.histogram_observe_us(
                            "rpc_route_registry_lookup_us",
                            route_registry_lookup_us,
                        );
                        self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
                        self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
                        self.histogram_observe_us("rpc_pending_track_us", pending_track_us);
                        self.histogram_observe_us("rpc_pending_route_index_us", 0);
                        self.gauge_set("rpc_pending_requests", pending_len);

                        let request_payload = crate::protocol::rpc_codec::encode_request_into(
                            &req,
                            &mut payload_encoder,
                        );
                        let request_forward_start = Instant::now();

                        let forward_ctx = FrameContext::new(
                            worker.session_id,
                            frame_ctx.channel_id,
                            crate::protocol::tlv::MessageType::new(302),
                            bytes::Bytes::from(request_payload),
                            *worker.inbox_addr.family(),
                        );
                        let forward_envelope = Envelope::new(worker.inbox_addr, forward_ctx);
                        let forward_result = self.router.route(forward_envelope);
                        self.histogram_observe_elapsed_us(
                            "rpc_request_forward_us",
                            request_forward_start,
                        );

                        match forward_result {
                            Ok(()) => {
                                self.counter_inc("rpc_requests_dispatched_total");
                                tracing::debug!(
                                    domain = "rpc",
                                    correlation_id = %req.correlation_id,
                                    route = req.route.as_str(),
                                    "Request forwarded to worker"
                                );
                                (Some(RpcResponseMsg::Ok { data: vec![] }), Some(false))
                            }
                            Err(crate::runtime::RouteError::RouteNotFound(_))
                            | Err(crate::runtime::RouteError::DeliveryFailed(
                                _,
                                DeliveryError::ActorStopped,
                            )) => {
                                self.counter_inc("rpc_request_forward_errors_total");
                                let cleanup_result = self.apply_session_cleanup(worker.session_id);
                                let disconnect_deliveries = cleanup_result
                                    .disconnect_deliveries
                                    .into_iter()
                                    .filter(|delivery| {
                                        delivery.correlation_id != req.correlation_id
                                    })
                                    .collect();
                                self.forward_worker_disconnect_errors(disconnect_deliveries);
                                tracing::warn!(
                                    domain = "rpc",
                                    correlation_id = %req.correlation_id,
                                    route = req.route.as_str(),
                                    worker_session_id = worker.session_id,
                                    "Worker disconnected before request dispatch completed"
                                );
                                (
                                    Some(RpcResponseMsg::CodeError {
                                        code:
                                            crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
                                        message: RPC_WORKER_NOT_FOUND_ERROR.to_string(),
                                    }),
                                    None,
                                )
                            }
                            Err(crate::runtime::RouteError::DeliveryFailed(
                                _,
                                DeliveryError::MailboxFull { .. },
                            ))
                            | Err(crate::runtime::RouteError::DeliveryFailed(
                                _,
                                DeliveryError::HighLaneFull { .. },
                            )) => {
                                self.counter_inc("rpc_request_forward_errors_total");
                                let pending_len = self.remove_pending_request(&req.correlation_id);
                                tracing::warn!(
                                    domain = "rpc",
                                    correlation_id = %req.correlation_id,
                                    route = req.route.as_str(),
                                    pending_len,
                                    "Failed to forward request to worker due to backpressure"
                                );
                                (
                                    Some(RpcResponseMsg::CodeError {
                                        code:
                                            crate::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
                                        message: RPC_BACKPRESSURE_ERROR.to_string(),
                                    }),
                                    None,
                                )
                            }
                        }
                    } else {
                        let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                        drop(state);

                        self.histogram_observe_us(
                            "rpc_route_registry_lookup_us",
                            route_registry_lookup_us,
                        );
                        self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
                        self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
                        self.counter_inc("rpc_requests_rejected_no_worker_total");
                        (
                            Some(RpcResponseMsg::CodeError {
                                code: crate::protocol::error_codes::rpc::ERR_ROUTE_NOT_REGISTERED,
                                message: RPC_NO_WORKERS_ERROR.to_string(),
                            }),
                            None,
                        )
                    }
                }
            }
            RpcMessage::Response(resp) => {
                self.counter_inc("rpc_responses_total");

                let state_wait_start = Instant::now();
                let mut state = self.state.lock();
                let state_wait_us = state_wait_start.elapsed().as_micros() as u64;
                let state_hold_start = Instant::now();
                let pending_route_lookup_start = Instant::now();
                let caller_info = state
                    .pending
                    .pending_for_response(&resp.correlation_id, resp.seq, resp.stream_end);
                let pending_route_lookup_us =
                    pending_route_lookup_start.elapsed().as_micros() as u64;
                let mut state_changed = false;

                match caller_info {
                    RpcPendingResponseDisposition::Forward {
                        pending: caller_info,
                        pending_len,
                        removed_pending,
                    } => {
                        let pending_lookup_us = pending_route_lookup_us;
                        if removed_pending {
                            let completion_latency_us =
                                caller_info.submitted_at_instant.elapsed().as_micros() as u64;
                            if let Some(route_state) =
                                state.route_state(caller_info.worker_addr.route())
                            {
                                route_state.record_completion(
                                    caller_info.worker_session_id,
                                    completion_latency_us,
                                );
                            }
                            self.histogram_observe_us(
                                "rpc_pending_route_remove_us",
                                pending_lookup_us,
                            );
                            self.histogram_observe_us("rpc_pending_untrack_us", pending_lookup_us);
                            self.gauge_set("rpc_pending_requests", pending_len as u64);
                            state_changed = true;
                        }

                        let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                        drop(state);

                        self.histogram_observe_us(
                            "rpc_pending_route_lookup_us",
                            pending_route_lookup_us,
                        );
                        self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
                        self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);

                        let response_forward_start = Instant::now();
                        let encoded_response = crate::protocol::rpc_codec::encode_response_message_into(
                            &resp,
                            &mut payload_encoder,
                        );
                        if let Some(caller_inbox_addr) = caller_info.caller_inbox_addr.as_ref() {
                            let forward_ctx = FrameContext::new(
                                caller_info.caller_session_id,
                                frame_ctx.channel_id,
                                crate::protocol::tlv::MessageType::new(303),
                                bytes::Bytes::from(encoded_response),
                                *caller_inbox_addr.family(),
                            );
                            let forward_envelope =
                                Envelope::new(caller_inbox_addr.clone(), forward_ctx);
                            if let Err(e) = self.router.route(forward_envelope) {
                                self.counter_inc("rpc_response_forward_errors_total");
                                tracing::warn!(
                                    domain = "rpc",
                                    correlation_id = %resp.correlation_id,
                                    error = ?e,
                                    "Failed to forward response to requester"
                                );
                            }
                        } else {
                            self.counter_inc("rpc_responses_dropped_closed_caller_total");
                        }
                        self.histogram_observe_elapsed_us(
                            "rpc_response_forward_us",
                            response_forward_start,
                        );

                        let ack_forward_start = Instant::now();
                        let ack_payload = crate::protocol::rpc_codec::encode_ack_into(
                            &resp.correlation_id,
                            &mut payload_encoder,
                        );
                        let ack_ctx = FrameContext::new(
                            frame_ctx.session_id,
                            frame_ctx.channel_id,
                            crate::protocol::tlv::MessageType::new(304),
                            bytes::Bytes::from(ack_payload),
                            RouteFamily::from_u32(envelope.destination().family().id()),
                        );
                        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
                            session_inbox_address(
                                *envelope.destination().family(),
                                frame_ctx.session_id,
                            )
                        });
                        let ack_envelope = Envelope::new(worker_inbox_addr, ack_ctx);
                        if let Err(e) = self.router.route(ack_envelope) {
                            self.counter_inc("rpc_ack_forward_errors_total");
                            tracing::warn!(
                                domain = "rpc",
                                correlation_id = %resp.correlation_id,
                                error = ?e,
                                "Failed to send ACK to worker"
                            );
                        } else {
                            self.counter_inc("rpc_worker_acks_total");
                        }
                        self.histogram_observe_elapsed_us("rpc_ack_forward_us", ack_forward_start);

                        tracing::debug!(
                            domain = "rpc",
                            correlation_id = %resp.correlation_id,
                            stream_end = resp.stream_end,
                            "Response forwarded to requester and ACK sent to worker"
                        );
                    }
                    RpcPendingResponseDisposition::InvalidSequence {
                        pending: caller_info,
                        pending_len,
                        expected_seq,
                    } => {
                        let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                        drop(state);
                        self.histogram_observe_us(
                            "rpc_pending_route_lookup_us",
                            pending_route_lookup_us,
                        );
                        self.histogram_observe_us(
                            "rpc_pending_route_remove_us",
                            pending_route_lookup_us,
                        );
                        self.histogram_observe_us("rpc_pending_untrack_us", pending_route_lookup_us);
                        self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
                        self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);
                        self.gauge_set("rpc_pending_requests", pending_len as u64);
                        self.counter_inc("rpc_response_invalid_sequence_total");
                        self.counter_inc("rpc_cleanup_pending_removed_total");
                        self.schedule_admin_snapshot(false);

                        if let Some(caller_inbox_addr) = caller_info.caller_inbox_addr {
                            self.forward_pending_error_deliveries(
                                vec![RpcPendingErrorDelivery {
                                    correlation_id: resp.correlation_id,
                                    caller_session_id: caller_info.caller_session_id,
                                    caller_inbox_addr,
                                }],
                                crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
                                RPC_INVALID_SEQUENCE_ERROR,
                                "rpc_invalid_sequence_errors_forwarded_total",
                                "rpc_invalid_sequence_errors_dropped_total",
                            );
                        } else {
                            self.counter_inc("rpc_invalid_sequence_errors_dropped_total");
                        }

                        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
                            session_inbox_address(
                                *envelope.destination().family(),
                                frame_ctx.session_id,
                            )
                        });
                        self.forward_pending_error_deliveries(
                            vec![RpcPendingErrorDelivery {
                                correlation_id: resp.correlation_id,
                                caller_session_id: frame_ctx.session_id,
                                caller_inbox_addr: worker_inbox_addr,
                            }],
                            crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
                            RPC_INVALID_SEQUENCE_ERROR,
                            "rpc_worker_protocol_errors_forwarded_total",
                            "rpc_worker_protocol_errors_dropped_total",
                        );

                        tracing::warn!(
                            domain = "rpc",
                            correlation_id = %resp.correlation_id,
                            expected_seq,
                            received_seq = resp.seq,
                            "Rejected RPC response with invalid sequence"
                        );
                    }
                    RpcPendingResponseDisposition::Missing => {
                        let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                        drop(state);
                        self.histogram_observe_us(
                            "rpc_pending_route_lookup_us",
                            pending_route_lookup_us,
                        );
                        self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
                        self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);
                        self.counter_inc("rpc_responses_missing_pending_total");
                        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
                            session_inbox_address(
                                *envelope.destination().family(),
                                frame_ctx.session_id,
                            )
                        });
                        self.forward_pending_error_deliveries(
                            vec![RpcPendingErrorDelivery {
                                correlation_id: resp.correlation_id,
                                caller_session_id: frame_ctx.session_id,
                                caller_inbox_addr: worker_inbox_addr,
                            }],
                            crate::protocol::error_codes::rpc::ERR_CORRELATION_NOT_FOUND,
                            RPC_CORRELATION_NOT_FOUND_ERROR,
                            "rpc_correlation_errors_forwarded_total",
                            "rpc_correlation_errors_dropped_total",
                        );
                        tracing::warn!(
                            domain = "rpc",
                            correlation_id = %resp.correlation_id,
                            "No pending request for response"
                        );
                    }
                }
                (None, state_changed.then_some(false))
            }
            RpcMessage::Ack { correlation_id } => {
                let state_wait_start = Instant::now();
                let mut state = self.state.lock();
                let state_wait_us = state_wait_start.elapsed().as_micros() as u64;
                let state_hold_start = Instant::now();
                let pending_route_remove_start = Instant::now();
                let pending_len = state.pending.remove_pending(&correlation_id);
                let pending_route_remove_us =
                    pending_route_remove_start.elapsed().as_micros() as u64;
                let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                drop(state);

                self.histogram_observe_us("rpc_pending_route_remove_us", pending_route_remove_us);
                self.histogram_observe_us("rpc_ack_state_wait_us", state_wait_us);
                self.histogram_observe_us("rpc_ack_state_hold_us", state_hold_us);
                if let Some(pending_len) = pending_len {
                    self.histogram_observe_us("rpc_pending_untrack_us", pending_route_remove_us);
                    self.gauge_set("rpc_pending_requests", pending_len as u64);
                    self.counter_inc("rpc_cleanup_acks_total");
                } else {
                    self.counter_inc("rpc_cleanup_acks_missing_pending_total");
                }
                tracing::debug!(
                    domain = "rpc",
                    correlation_id = %correlation_id,
                    "Request acknowledged and cleaned up"
                );
                (None, pending_len.is_some().then_some(false))
            }
            RpcMessage::Deliver(_) => (
                Some(RpcResponseMsg::Error(
                    "Deliver not valid client message".to_string(),
                )),
                None,
            ),
        };

        if let Some(force_snapshot) = snapshot_policy {
            self.schedule_admin_snapshot(force_snapshot);
        }

        if let Some(response) = response {
            let response_bytes =
                crate::protocol::rpc_codec::encode_response_into(&response, &mut payload_encoder);
            let response_ctx = FrameContext::new(
                frame_ctx.session_id,
                frame_ctx.channel_id,
                crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                bytes::Bytes::from(response_bytes),
                frame_ctx.route_family,
            );
            if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
                let _ = self.router.route(response_envelope);
            }
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_rpc_code_error(payload: &[u8], expected_code: u16, expected_message: &str) {
        let (code, message) =
            crate::protocol::rpc_codec::decode_error_body(payload).expect("rpc code error");
        assert_eq!(code, expected_code);
        assert_eq!(message, expected_message);
    }

    fn parse_forwarded_rpc_response(
        frame: &FrameContext,
    ) -> crate::domains::rpc::protocol::RpcResponse {
        match crate::protocol::rpc_codec::parse_request(frame, &frame.payload, frame.route_family)
            .expect("parse forwarded rpc response")
        {
            crate::domains::rpc::protocol::RpcMessage::Response(response) => response,
            other => panic!("expected rpc response, found {other:?}"),
        }
    }

    struct CaptureRpcFrameSink {
        frames: Arc<parking_lot::Mutex<Vec<FrameContext>>>,
    }

    fn test_rpc_worker(family: RouteFamily, route: &Route, session_id: u64) -> RpcWorker {
        RpcWorker::with_stats(
            RouteAddress::new(family, route.clone()),
            session_inbox_address(family, session_id),
            session_id,
            "2026-03-14T12:00:00Z",
            0,
            0,
        )
    }

    fn test_pending_request(
        family: RouteFamily,
        route: &Route,
        caller_session_id: u64,
        worker_session_id: u64,
        expires_at: Instant,
    ) -> RpcPendingRequest {
        let submitted_at_instant = Instant::now();
        RpcPendingRequest::new(RpcPendingRequestInit {
            route: route.clone(),
            caller_session_id,
            caller_inbox_addr: session_inbox_address(family, caller_session_id),
            worker_addr: RouteAddress::new(family, route.clone()),
            worker_session_id,
            submitted_at: "2026-03-14T12:00:00Z".to_string(),
            submitted_at_instant,
            expires_at,
        })
    }

    impl MailboxSink for CaptureRpcFrameSink {
        fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            if let Some(ctx) = envelope.payload::<FrameContext>() {
                self.frames.lock().push(ctx.clone());
            }
            Ok(())
        }

        fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            self.deliver(envelope)
        }
    }

    #[test]
    fn should_create_rpc_domain_sink() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = RpcDomainSink::new(router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_dispatch_workers_round_robin_given_route_local_rpc_state() {
        // Arrange
        let family = RouteFamily::new(1);
        let route = Route::new("rpc://bench/system/resource/operation");
        let mut route_state = RpcRouteState::new();
        route_state.register_worker(test_rpc_worker(family, &route, 10));
        route_state.register_worker(test_rpc_worker(family, &route, 11));

        // Act
        let first = route_state.select_worker().map(|worker| worker.session_id);
        let second = route_state.select_worker().map(|worker| worker.session_id);

        // Assert
        assert_eq!(first, Some(10));
        assert_eq!(second, Some(11));
    }

    #[test]
    fn should_reuse_route_state_given_equivalent_rpc_route_keys() {
        // Arrange
        let family = RouteFamily::new(1);
        let route = Route::new("rpc://bench/system/resource/operation");
        let duplicate_route = Route::new("rpc://bench/system/resource/operation");
        let mut state = RpcState::new();
        state
            .ensure_route_state(&route)
            .register_worker(test_rpc_worker(family, &route, 10));

        // Act
        let worker_count = state.ensure_route_state(&duplicate_route).worker_count();

        // Assert
        assert_eq!(worker_count, 1);
        assert_eq!(state.route_count(), 1);
    }

    #[test]
    fn should_schedule_rpc_admin_snapshot_when_interval_elapsed_given_dirty_state() {
        // Arrange
        let now_elapsed_us = RPC_ADMIN_SNAPSHOT_INTERVAL_US + 1;

        // Act
        let due = rpc_admin_snapshot_due(true, false, now_elapsed_us, 0);

        // Assert
        assert!(due);
    }

    #[test]
    fn should_skip_rpc_admin_snapshot_when_interval_not_elapsed_and_not_forced() {
        // Arrange
        let now_elapsed_us = RPC_ADMIN_SNAPSHOT_INTERVAL_US - 1;

        // Act
        let due = rpc_admin_snapshot_due(true, false, now_elapsed_us, 0);

        // Assert
        assert!(!due);
    }

    #[test]
    fn should_snapshot_live_pending_request_details_given_rpc_admin_snapshot() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = RpcDomainSink::new(router, admin_read_model.clone());
        let family = RouteFamily::new(1);
        let route = Route::new("rpc://prod/api/users/get");
        let correlation_id = uuid::Uuid::new_v4();

        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&route)
                .register_worker(test_rpc_worker(family, &route, 42));
            state.pending.track_pending(
                correlation_id,
                RpcPendingRequest::new(RpcPendingRequestInit {
                    route: route.clone(),
                    caller_session_id: 7,
                    caller_inbox_addr: session_inbox_address(family, 7),
                    worker_addr: RouteAddress::new(family, route.clone()),
                    worker_session_id: 42,
                    submitted_at: "2026-03-14T12:00:00Z".to_string(),
                    submitted_at_instant: Instant::now() - Duration::from_secs(9),
                    expires_at: Instant::now() + Duration::from_secs(30),
                }),
            );
        }

        // Act
        sink.sync_admin_snapshot();
        let pending = admin_read_model.rpc_pending(Some("prod"));

        // Assert
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].correlation_id, correlation_id.to_string());
        assert_eq!(pending[0].route, route.as_str());
        assert_eq!(pending[0].submitted_at, "2026-03-14T12:00:00Z");
        assert_eq!(pending[0].worker_session_id.as_deref(), Some("42"));
        assert!(pending[0].age_seconds >= 9);
    }

    #[test]
    fn should_snapshot_live_worker_metrics_after_terminal_response_given_rpc_admin_snapshot() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model.clone()));
        let family = RouteFamily::new(1);
        let route = Route::new("rpc://prod/api/users/get");
        let request_addr = RouteAddress::new(family, route.clone());
        let caller_addr = session_inbox_address(family, 7);
        let worker_inbox_addr = session_inbox_address(family, 42);
        let response_payload = crate::protocol::rpc_codec::encode_response_message(
            &crate::domains::rpc::protocol::RpcResponse::single(
                uuid::Uuid::new_v4(),
                bytes::Bytes::from_static(b"ok"),
            ),
        );
        let response = match crate::protocol::rpc_codec::parse_request(
            &FrameContext::new(
                42,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(response_payload.clone()),
                family,
            ),
            &response_payload,
            family,
        )
        .expect("parse rpc response")
        {
            crate::domains::rpc::protocol::RpcMessage::Response(response) => response,
            other => panic!("expected rpc response, found {other:?}"),
        };

        router.register(
            caller_addr.clone(),
            Arc::new(CaptureRpcFrameSink {
                frames: Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new())),
            }) as Arc<dyn MailboxSink>,
        );
        router.register(
            worker_inbox_addr.clone(),
            Arc::new(CaptureRpcFrameSink {
                frames: Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new())),
            }) as Arc<dyn MailboxSink>,
        );

        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&route)
                .register_worker(RpcWorker::with_stats(
                    request_addr.clone(),
                    worker_inbox_addr.clone(),
                    42,
                    "2026-03-14T11:59:00Z",
                    0,
                    0,
                ));
            let correlation_id = response.correlation_id;
            state.pending.track_pending(
                correlation_id,
                RpcPendingRequest::new(RpcPendingRequestInit {
                    route: route.clone(),
                    caller_session_id: 7,
                    caller_inbox_addr: caller_addr.clone(),
                    worker_addr: request_addr.clone(),
                    worker_session_id: 42,
                    submitted_at: "2026-03-14T12:00:00Z".to_string(),
                    submitted_at_instant: Instant::now() - Duration::from_millis(50),
                    expires_at: Instant::now() + Duration::from_secs(30),
                }),
            );
        }

        // Act
        sink.deliver(Envelope::from_route(
            worker_inbox_addr,
            request_addr,
            FrameContext::new(
                42,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(response_payload),
                family,
            ),
        ))
        .expect("deliver terminal response");
        sink.sync_admin_snapshot();
        let workers = admin_read_model.rpc_workers(Some("prod"));
        let pending = admin_read_model.rpc_pending(Some("prod"));

        // Assert
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].session_id, "42");
        assert_eq!(workers[0].route, route.as_str());
        assert_eq!(workers[0].registered_at, "2026-03-14T11:59:00Z");
        assert_eq!(workers[0].requests_handled, 1);
        assert!(workers[0].average_latency_ms >= 50.0);
        assert!(pending.is_empty());
    }

    #[test]
    fn should_accumulate_cleanup_counters_given_rpc_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let metrics = crate::observability::metrics::MetricsCollector::new();
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = RpcDomainSink::new(router, admin_read_model).with_metrics(metrics.clone());
        let worker_route_a = Route::new("rpc://bench/system/resource/operation-a");
        let worker_route_b = Route::new("rpc://bench/system/resource/operation-b");
        let external_route_a = Route::new("rpc://bench/external/resource/operation-a");
        let external_route_b = Route::new("rpc://bench/external/resource/operation-b");

        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&worker_route_a)
                .register_worker(test_rpc_worker(family, &worker_route_a, 42));
            state
                .ensure_route_state(&worker_route_b)
                .register_worker(test_rpc_worker(family, &worker_route_b, 42));
            state.pending.track_pending(
                uuid::Uuid::new_v4(),
                test_pending_request(
                    family,
                    &worker_route_a,
                    90,
                    42,
                    Instant::now() + Duration::from_secs(30),
                ),
            );
            state.pending.track_pending(
                uuid::Uuid::new_v4(),
                test_pending_request(
                    family,
                    &worker_route_b,
                    91,
                    42,
                    Instant::now() + Duration::from_secs(30),
                ),
            );
            state.pending.track_pending(
                uuid::Uuid::new_v4(),
                test_pending_request(
                    family,
                    &external_route_a,
                    42,
                    7,
                    Instant::now() + Duration::from_secs(30),
                ),
            );
            state.pending.track_pending(
                uuid::Uuid::new_v4(),
                test_pending_request(
                    family,
                    &external_route_b,
                    42,
                    8,
                    Instant::now() + Duration::from_secs(30),
                ),
            );
        }

        // Act
        let cleanup = sink.apply_session_cleanup(42);

        // Assert
        assert_eq!(cleanup.removed_workers, 2);
        assert_eq!(cleanup.detached_callers, 2);
        assert_eq!(cleanup.removed_pending, 2);
        assert_eq!(cleanup.pending_len, 2);
        assert_eq!(metrics.counter_get("rpc_cleanup_workers_removed_total"), 2);
        assert_eq!(metrics.counter_get("rpc_cleanup_callers_detached_total"), 2);
        assert_eq!(metrics.counter_get("rpc_cleanup_pending_removed_total"), 2);
        assert_eq!(metrics.gauge_get("rpc_pending_requests"), 2);
    }

    #[test]
    fn should_accumulate_pending_removed_counter_given_rpc_worker_unsubscribe() {
        // Arrange
        let family = RouteFamily::new(1);
        let metrics = crate::observability::metrics::MetricsCollector::new();
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = RpcDomainSink::new(router, admin_read_model).with_metrics(metrics.clone());
        let removed_route = Route::new("rpc://bench/system/resource/operation");
        let retained_route = Route::new("rpc://bench/system/resource/other");
        let removed_addr = RouteAddress::new(family, removed_route.clone());

        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&removed_route)
                .register_worker(test_rpc_worker(family, &removed_route, 42));
            state
                .ensure_route_state(&retained_route)
                .register_worker(test_rpc_worker(family, &retained_route, 42));
            state.pending.track_pending(
                uuid::Uuid::new_v4(),
                test_pending_request(
                    family,
                    &removed_route,
                    90,
                    42,
                    Instant::now() + Duration::from_secs(30),
                ),
            );
            state.pending.track_pending(
                uuid::Uuid::new_v4(),
                test_pending_request(
                    family,
                    &removed_route,
                    91,
                    42,
                    Instant::now() + Duration::from_secs(30),
                ),
            );
            state.pending.track_pending(
                uuid::Uuid::new_v4(),
                test_pending_request(
                    family,
                    &retained_route,
                    92,
                    42,
                    Instant::now() + Duration::from_secs(30),
                ),
            );
        }

        // Act
        let cleanup = sink.apply_worker_unsubscribe(&removed_addr, 42);

        // Assert
        assert_eq!(cleanup.removed_workers, 1);
        assert_eq!(cleanup.removed_pending, 2);
        assert_eq!(cleanup.pending_len, 1);
        assert_eq!(metrics.counter_get("rpc_cleanup_workers_removed_total"), 1);
        assert_eq!(metrics.counter_get("rpc_cleanup_pending_removed_total"), 2);
        assert_eq!(metrics.gauge_get("rpc_pending_requests"), 1);
    }

    #[test]
    fn should_accumulate_timeout_counters_given_rpc_timeout_sweep() {
        // Arrange
        let family = RouteFamily::new(1);
        let metrics = crate::observability::metrics::MetricsCollector::new();
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = RpcDomainSink::new(router.clone(), admin_read_model)
            .with_metrics(metrics.clone())
            .with_request_timeout(Duration::from_millis(10));
        let caller_one = session_inbox_address(family, 1);
        let caller_two = session_inbox_address(family, 2);
        let caller_sink = Arc::new(CaptureRpcFrameSink {
            frames: Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new())),
        });
        router.register(
            caller_one.clone(),
            caller_sink.clone() as Arc<dyn MailboxSink>,
        );
        router.register(caller_two.clone(), caller_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state.pending.track_pending(
                uuid::Uuid::new_v4(),
                RpcPendingRequest::new(RpcPendingRequestInit {
                    route: Route::new("rpc://bench/system/resource/a"),
                    caller_session_id: 1,
                    caller_inbox_addr: caller_one,
                    worker_addr: RouteAddress::new(
                        family,
                        Route::new("rpc://bench/system/resource/a"),
                    ),
                    worker_session_id: 42,
                    submitted_at: "2026-03-14T12:00:00Z".to_string(),
                    submitted_at_instant: Instant::now(),
                    expires_at: Instant::now() + Duration::from_millis(5),
                }),
            );
            state.pending.track_pending(
                uuid::Uuid::new_v4(),
                RpcPendingRequest::new(RpcPendingRequestInit {
                    route: Route::new("rpc://bench/system/resource/b"),
                    caller_session_id: 2,
                    caller_inbox_addr: caller_two,
                    worker_addr: RouteAddress::new(
                        family,
                        Route::new("rpc://bench/system/resource/b"),
                    ),
                    worker_session_id: 43,
                    submitted_at: "2026-03-14T12:00:00Z".to_string(),
                    submitted_at_instant: Instant::now(),
                    expires_at: Instant::now() + Duration::from_millis(5),
                }),
            );
        }

        // Act
        sink.expire_timed_out_requests_at(Instant::now() + Duration::from_millis(25));

        // Assert
        assert_eq!(sink.pending_request_count(), 0);
        assert_eq!(metrics.counter_get("rpc_request_timeouts_total"), 2);
        assert_eq!(metrics.counter_get("rpc_cleanup_pending_removed_total"), 2);
        assert_eq!(metrics.gauge_get("rpc_pending_requests"), 0);
    }

    #[test]
    fn should_forward_timeout_error_given_expired_pending_request() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(
            RpcDomainSink::new(router.clone(), admin_read_model)
                .with_request_timeout(Duration::from_millis(10)),
        );
        let family = RouteFamily::new(1);
        let request_route = Route::new("rpc://bench/system/resource/timeout");
        let request_addr = RouteAddress::new(family, request_route.clone());
        let request_source = session_inbox_address(family, 1);
        let worker_source = session_inbox_address(family, 42);
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        let worker_sink = Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        });
        router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
        router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&request_route)
                .register_worker(test_rpc_worker(family, &request_route, 42));
        }
        let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);
        let request_ctx = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(request_msg_type),
            request_payload,
            family,
        );

        sink.deliver(Envelope::from_route(
            request_source,
            request_addr,
            request_ctx,
        ))
        .expect("deliver request");

        // Act
        sink.expire_timed_out_requests_at(Instant::now() + Duration::from_millis(25));

        // Assert
        assert_eq!(sink.pending_request_count(), 0);
        assert_eq!(worker_frames.lock().len(), 1);
        let reply_frames = reply_frames.lock();
        assert_eq!(reply_frames.len(), 2);
        assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
        assert_eq!(reply_frames[0].payload[0], 0);
        assert_eq!(reply_frames[1].msg_type.as_u16(), 303);
        let error_response = parse_forwarded_rpc_response(&reply_frames[1]);
        assert!(error_response.stream_end);
        assert_rpc_code_error(
            error_response.body.as_ref(),
            crate::protocol::error_codes::rpc::ERR_RPC_TIMEOUT,
            RPC_TIMEOUT_ERROR,
        );
    }

    #[test]
    fn should_drop_timeout_error_given_requester_cleanup_before_expiration() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(
            RpcDomainSink::new(router.clone(), admin_read_model)
                .with_request_timeout(Duration::from_millis(10)),
        );
        let family = RouteFamily::new(1);
        let request_route = Route::new("rpc://bench/system/resource/timeout");
        let request_addr = RouteAddress::new(family, request_route.clone());
        let request_source = session_inbox_address(family, 1);
        let worker_source = session_inbox_address(family, 42);
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        let worker_sink = Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        });
        router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
        router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&request_route)
                .register_worker(test_rpc_worker(family, &request_route, 42));
        }
        let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);
        let request_ctx = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(request_msg_type),
            request_payload,
            family,
        );

        sink.deliver(Envelope::from_route(
            request_source.clone(),
            request_addr,
            request_ctx,
        ))
        .expect("deliver request");
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("rpc://cleanup")),
            crate::runtime::SessionCleanup { session_id: 1 },
        ))
        .expect("cleanup requester session");

        // Act
        sink.expire_timed_out_requests_at(Instant::now() + Duration::from_millis(25));

        // Assert
        assert_eq!(sink.pending_request_count(), 0);
        assert_eq!(worker_frames.lock().len(), 1);
        let reply_frames = reply_frames.lock();
        assert_eq!(reply_frames.len(), 1);
        assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
        assert_eq!(reply_frames[0].payload[0], 0);
    }

    #[test]
    fn should_reject_rpc_request_when_pending_capacity_reached() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
        let family = RouteFamily::new(1);
        let request_route = Route::new("rpc://bench/system/resource/operation");
        let source_addr = session_inbox_address(family, 1);
        let worker_inbox_addr = session_inbox_address(family, 42);
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        let worker_sink = Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        });
        router.register(source_addr.clone(), reply_sink as Arc<dyn MailboxSink>);
        router.register(worker_inbox_addr, worker_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&request_route)
                .register_worker(test_rpc_worker(family, &request_route, 42));
            for _ in 0..RPC_MAX_PENDING_REQUESTS {
                state.pending.track_pending(
                    uuid::Uuid::new_v4(),
                    test_pending_request(
                        family,
                        &request_route,
                        7,
                        42,
                        Instant::now() + Duration::from_secs(30),
                    ),
                );
            }
        }
        let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"payload");
        let (msg_type, payload) = crate::benchkit::extract_single_tlv_field(&request_frame);
        let frame_ctx = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(msg_type),
            payload,
            family,
        );
        let request_addr = RouteAddress::new(family, request_route);
        let envelope = Envelope::from_route(source_addr, request_addr, frame_ctx);

        // Act
        let result = sink.deliver(envelope);

        // Assert
        assert!(result.is_ok());
        assert_eq!(sink.pending_request_count(), RPC_MAX_PENDING_REQUESTS);
        assert!(worker_frames.lock().is_empty());
        let reply_frames = reply_frames.lock();
        assert_eq!(reply_frames.len(), 1);
        assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
        assert_rpc_code_error(
            &reply_frames[0].payload,
            crate::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
            RPC_BACKPRESSURE_ERROR,
        );
    }

    #[test]
    fn should_reject_request_when_worker_disconnects_before_dispatch_given_missing_worker_inbox() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
        let family = RouteFamily::new(1);
        let request_route = Route::new("rpc://bench/system/resource/operation");
        let request_addr = RouteAddress::new(family, request_route.clone());
        let request_source = session_inbox_address(family, 1);
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&request_route)
                .register_worker(test_rpc_worker(family, &request_route, 42));
        }
        let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);

        // Act
        sink.deliver(Envelope::from_route(
            request_source,
            request_addr,
            FrameContext::new(
                1,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(request_msg_type),
                request_payload,
                family,
            ),
        ))
        .expect("deliver request");

        // Assert
        assert_eq!(sink.pending_request_count(), 0);
        assert_eq!(sink.worker_count(), 0);
        let reply_frames = reply_frames.lock();
        assert_eq!(reply_frames.len(), 1);
        assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
        assert_rpc_code_error(
            &reply_frames[0].payload,
            crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
            RPC_WORKER_NOT_FOUND_ERROR,
        );
    }

    #[test]
    fn should_forward_response_to_original_request_source_given_noncanonical_inbox_route() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
        let family = RouteFamily::new(1);
        let request_route = Route::new("rpc://bench/system/resource/operation");
        let request_addr = RouteAddress::new(family, request_route.clone());
        let request_source = RouteAddress::new(family, Route::new("inbox://session/1/custom"));
        let worker_source = RouteAddress::new(family, Route::new("inbox://session/42/custom"));
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        let worker_sink = Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        });
        router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
        router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&request_route)
                .register_worker(RpcWorker::with_stats(
                    request_addr.clone(),
                    worker_source.clone(),
                    42,
                    "2026-03-14T12:00:00Z",
                    0,
                    0,
                ));
        }
        let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);
        let request_ctx = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(request_msg_type),
            request_payload.clone(),
            family,
        );
        let request =
            match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
                .expect("parse rpc request")
            {
                crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
                other => panic!("expected rpc request, found {other:?}"),
            };
        let response_payload = crate::protocol::rpc_codec::encode_response_message(
            &crate::domains::rpc::protocol::RpcResponse::single(
                request.correlation_id,
                bytes::Bytes::from_static(b"ok"),
            ),
        );

        // Act
        sink.deliver(Envelope::from_route(
            request_source.clone(),
            request_addr.clone(),
            request_ctx,
        ))
        .expect("deliver request");
        sink.deliver(Envelope::from_route(
            worker_source,
            request_addr,
            FrameContext::new(
                42,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(response_payload),
                family,
            ),
        ))
        .expect("deliver response");

        // Assert
        let reply_frames = reply_frames.lock();
        assert!(
            reply_frames
                .iter()
                .any(|frame| frame.msg_type.as_u16() == 303),
            "expected worker response on original request source route"
        );
    }

    #[test]
    fn should_detach_caller_pending_given_rpc_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let caller_inbox_addr = session_inbox_address(family, 7);
        let mut state = RpcState::new();
        let route = Route::new("rpc://bench/system/resource/operation");
        state
            .ensure_route_state(&route)
            .register_worker(test_rpc_worker(family, &route, 42));
        let detached_correlation_id = uuid::Uuid::new_v4();
        state.pending.track_pending(
            detached_correlation_id,
            RpcPendingRequest::new(RpcPendingRequestInit {
                route: route.clone(),
                caller_session_id: 7,
                caller_inbox_addr: caller_inbox_addr.clone(),
                worker_addr: RouteAddress::new(family, route.clone()),
                worker_session_id: 42,
                submitted_at: "2026-03-14T12:00:00Z".to_string(),
                submitted_at_instant: Instant::now(),
                expires_at: Instant::now() + Duration::from_secs(30),
            }),
        );

        // Act
        let caller_cleanup = state.cleanup_session(7);

        // Assert
        assert_eq!(caller_cleanup.removed_workers, 0);
        assert_eq!(caller_cleanup.detached_callers, 1);
        assert_eq!(caller_cleanup.removed_pending, 0);
        assert_eq!(caller_cleanup.pending_len, 1);
        let detached_pending = state
            .pending
            .pending
            .get(&detached_correlation_id)
            .expect("detached pending should remain tracked");
        assert_eq!(detached_pending.caller_session_id, 7);
        assert_eq!(detached_pending.caller_inbox_addr, None);
        assert_eq!(
            detached_pending.worker_addr,
            RouteAddress::new(family, route)
        );
        assert_eq!(detached_pending.worker_session_id, 42);
        assert_eq!(state.pending.len(), 1);
        assert_eq!(state.route_count(), 1);
    }

    #[test]
    fn should_remove_worker_entries_given_rpc_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let mut state = RpcState::new();
        let route = Route::new("rpc://bench/system/resource/operation");
        state
            .ensure_route_state(&route)
            .register_worker(test_rpc_worker(family, &route, 42));

        // Act
        let worker_cleanup = state.cleanup_session(42);

        // Assert
        assert_eq!(worker_cleanup.removed_workers, 1);
        assert_eq!(worker_cleanup.detached_callers, 0);
        assert_eq!(worker_cleanup.removed_pending, 0);
        assert_eq!(worker_cleanup.pending_len, 0);
        assert_eq!(state.route_count(), 0);
    }

    #[test]
    fn should_remove_worker_owned_pending_given_rpc_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let mut state = RpcState::new();
        let worker_route = Route::new("rpc://bench/system/resource/operation");
        state.pending.track_pending(
            uuid::Uuid::new_v4(),
            test_pending_request(
                family,
                &worker_route,
                99,
                42,
                Instant::now() + Duration::from_secs(30),
            ),
        );

        // Act
        let worker_cleanup = state.cleanup_session(42);

        // Assert
        assert_eq!(worker_cleanup.removed_workers, 0);
        assert_eq!(worker_cleanup.detached_callers, 0);
        assert_eq!(worker_cleanup.removed_pending, 1);
        assert_eq!(worker_cleanup.pending_len, 0);
        assert_eq!(state.pending.len(), 0);
    }

    #[test]
    fn should_remove_only_matching_pending_given_worker_unsubscribe() {
        // Arrange
        let family = RouteFamily::new(1);
        let mut state = RpcState::new();
        let removed_route = Route::new("rpc://bench/system/resource/operation");
        let retained_route = Route::new("rpc://bench/system/resource/other");
        let removed_worker_addr = RouteAddress::new(family, removed_route.clone());
        let retained_worker_addr = RouteAddress::new(family, retained_route.clone());
        let removed_correlation_id = uuid::Uuid::new_v4();
        let retained_correlation_id = uuid::Uuid::new_v4();

        state
            .ensure_route_state(&removed_route)
            .register_worker(test_rpc_worker(family, &removed_route, 42));
        state
            .ensure_route_state(&retained_route)
            .register_worker(test_rpc_worker(family, &retained_route, 42));
        state.pending.track_pending(
            removed_correlation_id,
            test_pending_request(
                family,
                &removed_route,
                99,
                42,
                Instant::now() + Duration::from_secs(30),
            ),
        );
        state.pending.track_pending(
            retained_correlation_id,
            test_pending_request(
                family,
                &retained_route,
                100,
                42,
                Instant::now() + Duration::from_secs(30),
            ),
        );

        // Act
        let cleanup = state.unregister_worker(&removed_worker_addr, 42);

        // Assert
        assert_eq!(cleanup.removed_workers, 1);
        assert_eq!(cleanup.removed_pending, 1);
        assert_eq!(cleanup.pending_len, 1);
        assert_eq!(cleanup.disconnect_deliveries.len(), 1);
        assert_eq!(
            cleanup.disconnect_deliveries[0].correlation_id,
            removed_correlation_id
        );
        assert!(
            !state.pending.pending.contains_key(&removed_correlation_id),
            "removed worker pending should no longer be tracked"
        );
        let retained_pending = state
            .pending
            .pending
            .get(&retained_correlation_id)
            .expect("retained worker pending should remain tracked");
        assert_eq!(retained_pending.worker_addr, retained_worker_addr);
        assert_eq!(state.pending.len(), 1);
        assert_eq!(state.route_count(), 1);
    }

    #[test]
    fn should_forward_worker_disconnect_error_given_rpc_unsubscribe() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
        let family = RouteFamily::new(1);
        let request_route = Route::new("rpc://bench/system/resource/operation");
        let request_addr = RouteAddress::new(family, request_route.clone());
        let request_source = session_inbox_address(family, 1);
        let worker_source = session_inbox_address(family, 42);
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        let worker_sink = Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        });
        router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
        router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&request_route)
                .register_worker(RpcWorker::with_stats(
                    request_addr.clone(),
                    worker_source.clone(),
                    42,
                    "2026-03-14T12:00:00Z",
                    0,
                    0,
                ));
        }
        let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);
        let request_ctx = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(request_msg_type),
            request_payload.clone(),
            family,
        );
        let request =
            match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
                .expect("parse rpc request")
            {
                crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
                other => panic!("expected rpc request, found {other:?}"),
            };
        let unsubscribe_payload = {
            let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
            encoder.put_string(request_route.as_str());
            encoder.finish()
        };

        // Act
        sink.deliver(Envelope::from_route(
            request_source,
            request_addr.clone(),
            request_ctx,
        ))
        .expect("deliver request");
        sink.deliver(Envelope::from_route(
            worker_source,
            request_addr,
            FrameContext::new(
                42,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(301),
                bytes::Bytes::from(unsubscribe_payload),
                family,
            ),
        ))
        .expect("unsubscribe worker");

        // Assert
        assert_eq!(sink.pending_request_count(), 0);
        assert_eq!(sink.worker_count(), 0);
        let reply_frames = reply_frames.lock();
        assert_eq!(reply_frames.len(), 2);
        assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
        assert_eq!(reply_frames[0].payload[0], 0);
        assert_eq!(reply_frames[1].msg_type.as_u16(), 303);
        let error_response = parse_forwarded_rpc_response(&reply_frames[1]);
        assert_eq!(error_response.correlation_id, request.correlation_id);
        assert_eq!(error_response.seq, 0);
        assert!(error_response.stream_end);
        assert_rpc_code_error(
            error_response.body.as_ref(),
            crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
            RPC_WORKER_NOT_FOUND_ERROR,
        );
    }

    #[test]
    fn should_retain_other_worker_route_given_rpc_unsubscribe_on_same_session() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
        let family = RouteFamily::new(1);
        let removed_route = Route::new("rpc://bench/system/resource/operation");
        let retained_route = Route::new("rpc://bench/system/resource/other");
        let removed_addr = RouteAddress::new(family, removed_route.clone());
        let retained_addr = RouteAddress::new(family, retained_route.clone());
        let request_source = session_inbox_address(family, 1);
        let worker_source = session_inbox_address(family, 42);
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        let worker_sink = Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        });
        router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
        router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&removed_route)
                .register_worker(RpcWorker::with_stats(
                    removed_addr.clone(),
                    worker_source.clone(),
                    42,
                    "2026-03-14T12:00:00Z",
                    0,
                    0,
                ));
            state
                .ensure_route_state(&retained_route)
                .register_worker(RpcWorker::with_stats(
                    retained_addr.clone(),
                    worker_source.clone(),
                    42,
                    "2026-03-14T12:00:00Z",
                    0,
                    0,
                ));
        }
        let unsubscribe_payload = {
            let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
            encoder.put_string(removed_route.as_str());
            encoder.finish()
        };
        let request_frame = crate::benchkit::build_rpc_request(retained_route.as_str(), b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);
        let request_ctx = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(request_msg_type),
            request_payload.clone(),
            family,
        );
        let request =
            match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
                .expect("parse rpc request")
            {
                crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
                other => panic!("expected rpc request, found {other:?}"),
            };
        let response_payload = crate::protocol::rpc_codec::encode_response_message(
            &crate::domains::rpc::protocol::RpcResponse::single(
                request.correlation_id,
                bytes::Bytes::from_static(b"ok"),
            ),
        );

        // Act
        sink.deliver(Envelope::from_route(
            worker_source.clone(),
            removed_addr,
            FrameContext::new(
                42,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(301),
                bytes::Bytes::from(unsubscribe_payload),
                family,
            ),
        ))
        .expect("unsubscribe removed worker route");
        sink.deliver(Envelope::from_route(
            request_source,
            retained_addr.clone(),
            request_ctx,
        ))
        .expect("dispatch request to retained route");
        sink.deliver(Envelope::from_route(
            worker_source,
            retained_addr,
            FrameContext::new(
                42,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(response_payload),
                family,
            ),
        ))
        .expect("deliver retained worker response");

        // Assert
        assert_eq!(sink.worker_count(), 1);
        assert_eq!(sink.pending_request_count(), 0);

        let reply_frames = reply_frames.lock();
        assert_eq!(reply_frames.len(), 2);
        assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
        assert_eq!(reply_frames[0].payload[0], 0);
        assert_eq!(reply_frames[1].msg_type.as_u16(), 303);
        let forwarded_response = parse_forwarded_rpc_response(&reply_frames[1]);
        assert_eq!(forwarded_response.correlation_id, request.correlation_id);
        assert_eq!(forwarded_response.seq, 0);
        assert!(forwarded_response.stream_end);
        assert_eq!(forwarded_response.body.as_ref(), b"ok");

        let worker_frames = worker_frames.lock();
        assert_eq!(worker_frames.len(), 3);
        assert_eq!(worker_frames[0].msg_type.as_u16(), 301);
        assert_eq!(worker_frames[0].payload[0], 0);
        assert_eq!(worker_frames[1].msg_type.as_u16(), 302);
        assert_eq!(worker_frames[2].msg_type.as_u16(), 304);
    }

    #[test]
    fn should_forward_worker_disconnect_error_given_rpc_session_cleanup() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
        let family = RouteFamily::new(1);
        let request_route = Route::new("rpc://bench/system/resource/operation");
        let request_addr = RouteAddress::new(family, request_route.clone());
        let request_source = session_inbox_address(family, 1);
        let worker_source = session_inbox_address(family, 42);
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        let worker_sink = Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        });
        router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
        router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&request_route)
                .register_worker(test_rpc_worker(family, &request_route, 42));
        }
        let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);
        let request_ctx = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(request_msg_type),
            request_payload.clone(),
            family,
        );
        let request =
            match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
                .expect("parse rpc request")
            {
                crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
                other => panic!("expected rpc request, found {other:?}"),
            };

        // Act
        sink.deliver(Envelope::from_route(
            request_source,
            request_addr,
            request_ctx,
        ))
        .expect("deliver request");
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("rpc://cleanup")),
            crate::runtime::SessionCleanup { session_id: 42 },
        ))
        .expect("cleanup worker session");

        // Assert
        assert_eq!(sink.pending_request_count(), 0);
        assert_eq!(sink.worker_count(), 0);
        let reply_frames = reply_frames.lock();
        assert_eq!(reply_frames.len(), 2);
        assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
        assert_eq!(reply_frames[0].payload[0], 0);
        assert_eq!(reply_frames[1].msg_type.as_u16(), 303);
        let error_response = parse_forwarded_rpc_response(&reply_frames[1]);
        assert_eq!(error_response.correlation_id, request.correlation_id);
        assert_eq!(error_response.seq, 0);
        assert!(error_response.stream_end);
        assert_rpc_code_error(
            error_response.body.as_ref(),
            crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
            RPC_WORKER_NOT_FOUND_ERROR,
        );
    }

    #[test]
    fn should_reject_worker_response_when_correlation_missing_given_rpc_sink() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(
            RpcDomainSink::new(router.clone(), admin_read_model)
                .with_request_timeout(Duration::from_millis(250)),
        );
        let family = RouteFamily::new(1);
        let request_route = Route::new("rpc://bench/system/resource/operation");
        let request_addr = RouteAddress::new(family, request_route.clone());
        let request_source = session_inbox_address(family, 1);
        let worker_source = session_inbox_address(family, 42);
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        let worker_sink = Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        });
        router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
        router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&request_route)
                .register_worker(RpcWorker::with_stats(
                    request_addr.clone(),
                    worker_source.clone(),
                    42,
                    "2026-03-14T12:00:00Z",
                    0,
                    0,
                ));
        }
        let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);
        let request_ctx = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(request_msg_type),
            request_payload,
            family,
        );
        let orphan_correlation_id = uuid::Uuid::new_v4();
        let orphan_response_payload = crate::protocol::rpc_codec::encode_response_message(
            &crate::domains::rpc::protocol::RpcResponse::single(
                orphan_correlation_id,
                bytes::Bytes::from_static(b"wrong"),
            ),
        );

        // Act
        sink.deliver(Envelope::from_route(
            request_source,
            request_addr.clone(),
            request_ctx,
        ))
        .expect("deliver request");
        sink.deliver(Envelope::from_route(
            worker_source,
            request_addr,
            FrameContext::new(
                42,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(orphan_response_payload),
                family,
            ),
        ))
        .expect("deliver orphan response");

        // Assert
        assert_eq!(sink.pending_request_count(), 1);
        let reply_frames = reply_frames.lock();
        assert_eq!(reply_frames.len(), 1);
        assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
        let worker_frames = worker_frames.lock();
        assert_eq!(worker_frames.len(), 2);
        assert_eq!(worker_frames[0].msg_type.as_u16(), 302);
        assert_eq!(worker_frames[1].msg_type.as_u16(), 303);
        let error_response = parse_forwarded_rpc_response(&worker_frames[1]);
        assert_eq!(error_response.correlation_id, orphan_correlation_id);
        assert_eq!(error_response.seq, 0);
        assert!(error_response.stream_end);
        assert_rpc_code_error(
            error_response.body.as_ref(),
            crate::protocol::error_codes::rpc::ERR_CORRELATION_NOT_FOUND,
            RPC_CORRELATION_NOT_FOUND_ERROR,
        );
    }

    #[test]
    fn should_drop_late_worker_response_after_requester_cleanup_without_forward_error() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
        let family = RouteFamily::new(1);
        let request_route = Route::new("rpc://bench/system/resource/operation");
        let request_addr = RouteAddress::new(family, request_route.clone());
        let request_source = session_inbox_address(family, 1);
        let worker_source = session_inbox_address(family, 42);
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        let worker_sink = Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        });
        router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
        router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&request_route)
                .register_worker(RpcWorker::with_stats(
                    request_addr.clone(),
                    worker_source.clone(),
                    42,
                    "2026-03-14T12:00:00Z",
                    0,
                    0,
                ));
        }
        let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);
        let request_ctx = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(request_msg_type),
            request_payload.clone(),
            family,
        );
        let request =
            match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
                .expect("parse rpc request")
            {
                crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
                other => panic!("expected rpc request, found {other:?}"),
            };
        let response_payload = crate::protocol::rpc_codec::encode_response_message(
            &crate::domains::rpc::protocol::RpcResponse::single(
                request.correlation_id,
                bytes::Bytes::from_static(b"ok"),
            ),
        );

        // Act
        sink.deliver(Envelope::from_route(
            request_source.clone(),
            request_addr.clone(),
            request_ctx,
        ))
        .expect("deliver request");
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("rpc://cleanup")),
            crate::runtime::SessionCleanup { session_id: 1 },
        ))
        .expect("cleanup requester session");
        router.unregister(&request_source);
        sink.deliver(Envelope::from_route(
            worker_source,
            request_addr,
            FrameContext::new(
                42,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(response_payload),
                family,
            ),
        ))
        .expect("deliver response");

        // Assert
        let reply_frames = reply_frames.lock();
        assert_eq!(reply_frames.len(), 1);
        assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
        let worker_frames = worker_frames.lock();
        assert!(
            worker_frames
                .iter()
                .any(|frame| frame.msg_type.as_u16() == 304),
            "expected worker ACK even when requester has disconnected"
        );
    }

    #[test]
    fn should_remove_pending_request_on_stream_end_given_rpc_pending_table() {
        // Arrange
        let correlation_id = uuid::Uuid::new_v4();
        let caller_inbox_addr = session_inbox_address(RouteFamily::new(7), 42);
        let worker_addr = RouteAddress::new(
            RouteFamily::new(7),
            Route::new("rpc://bench/system/resource/operation"),
        );
        let mut pending = RpcPendingTable::new();
        let pending_len = pending.track_pending(
            correlation_id,
            RpcPendingRequest::new(RpcPendingRequestInit {
                route: worker_addr.route().clone(),
                caller_session_id: 42,
                caller_inbox_addr: caller_inbox_addr.clone(),
                worker_addr: worker_addr.clone(),
                worker_session_id: 77,
                submitted_at: "2026-03-14T12:00:00Z".to_string(),
                submitted_at_instant: Instant::now(),
                expires_at: Instant::now() + Duration::from_secs(30),
            }),
        );

        // Act
        let result = pending.pending_for_response(&correlation_id, 0, true);

        // Assert
        assert_eq!(pending_len, 1);
        match result {
            RpcPendingResponseDisposition::Forward {
                pending: tracked,
                pending_len,
                removed_pending,
            } => {
                assert_eq!(tracked.caller_session_id, 42);
                assert_eq!(tracked.caller_inbox_addr, Some(caller_inbox_addr));
                assert_eq!(tracked.worker_addr, worker_addr);
                assert_eq!(tracked.worker_session_id, 77);
                assert!(removed_pending);
                assert_eq!(pending_len, 0);
                assert_eq!(pending.len(), 0);
            }
            other => panic!("expected terminal response handling, found {other:?}"),
        }
    }

    #[test]
    fn should_retain_pending_request_before_stream_end_given_rpc_pending_table() {
        // Arrange
        let correlation_id = uuid::Uuid::new_v4();
        let caller_inbox_addr = session_inbox_address(RouteFamily::new(9), 84);
        let worker_addr = RouteAddress::new(
            RouteFamily::new(9),
            Route::new("rpc://bench/system/resource/operation"),
        );
        let mut pending = RpcPendingTable::new();
        let pending_len = pending.track_pending(
            correlation_id,
            RpcPendingRequest::new(RpcPendingRequestInit {
                route: worker_addr.route().clone(),
                caller_session_id: 84,
                caller_inbox_addr: caller_inbox_addr.clone(),
                worker_addr: worker_addr.clone(),
                worker_session_id: 99,
                submitted_at: "2026-03-14T12:00:00Z".to_string(),
                submitted_at_instant: Instant::now(),
                expires_at: Instant::now() + Duration::from_secs(30),
            }),
        );

        // Act
        let result = pending.pending_for_response(&correlation_id, 0, false);

        // Assert
        assert_eq!(pending_len, 1);
        match result {
            RpcPendingResponseDisposition::Forward {
                pending: tracked,
                pending_len,
                removed_pending,
            } => {
                assert_eq!(tracked.caller_session_id, 84);
                assert_eq!(tracked.caller_inbox_addr, Some(caller_inbox_addr));
                assert_eq!(tracked.worker_addr, worker_addr);
                assert_eq!(tracked.worker_session_id, 99);
                assert!(!removed_pending);
                assert_eq!(pending_len, 1);
                assert_eq!(pending.len(), 1);
                assert_eq!(pending.pending[&correlation_id].next_expected_seq, 1);
            }
            other => panic!("expected non-terminal response handling, found {other:?}"),
        }
    }

    #[test]
    fn should_reject_invalid_response_sequence_given_rpc_pending_table() {
        // Arrange
        let correlation_id = uuid::Uuid::new_v4();
        let caller_inbox_addr = session_inbox_address(RouteFamily::new(11), 21);
        let worker_addr = RouteAddress::new(
            RouteFamily::new(11),
            Route::new("rpc://bench/system/resource/operation"),
        );
        let mut pending = RpcPendingTable::new();
        pending.track_pending(
            correlation_id,
            RpcPendingRequest::new(RpcPendingRequestInit {
                route: worker_addr.route().clone(),
                caller_session_id: 21,
                caller_inbox_addr,
                worker_addr,
                worker_session_id: 77,
                submitted_at: "2026-03-14T12:00:00Z".to_string(),
                submitted_at_instant: Instant::now(),
                expires_at: Instant::now() + Duration::from_secs(30),
            }),
        );

        // Act
        let result = pending.pending_for_response(&correlation_id, 1, false);

        // Assert
        match result {
            RpcPendingResponseDisposition::InvalidSequence {
                pending: tracked,
                pending_len,
                expected_seq,
            } => {
                assert_eq!(tracked.caller_session_id, 21);
                assert_eq!(tracked.worker_session_id, 77);
                assert_eq!(expected_seq, 0);
                assert_eq!(pending_len, 0);
                assert_eq!(pending.len(), 0);
            }
            other => panic!("expected invalid sequence handling, found {other:?}"),
        }
    }

    #[test]
    fn should_reject_out_of_order_worker_response_given_rpc_sink() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
        let family = RouteFamily::new(1);
        let request_route = Route::new("rpc://bench/system/resource/operation");
        let request_addr = RouteAddress::new(family, request_route.clone());
        let request_source = session_inbox_address(family, 1);
        let worker_source = session_inbox_address(family, 42);
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        let worker_sink = Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        });
        router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
        router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&request_route)
                .register_worker(test_rpc_worker(family, &request_route, 42));
        }
        let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);
        let request_ctx = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(request_msg_type),
            request_payload.clone(),
            family,
        );
        let request =
            match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
                .expect("parse rpc request")
            {
                crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
                other => panic!("expected rpc request, found {other:?}"),
            };
        let invalid_response_payload = crate::protocol::rpc_codec::encode_response_message(
            &crate::domains::rpc::protocol::RpcResponse::chunk(
                request.correlation_id,
                1,
                bytes::Bytes::from_static(b"gap"),
                false,
            ),
        );

        // Act
        sink.deliver(Envelope::from_route(
            request_source,
            request_addr.clone(),
            request_ctx,
        ))
        .expect("deliver request");
        sink.deliver(Envelope::from_route(
            worker_source,
            request_addr,
            FrameContext::new(
                42,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(invalid_response_payload),
                family,
            ),
        ))
        .expect("deliver invalid response");

        // Assert
        assert_eq!(sink.pending_request_count(), 0);

        let reply_frames = reply_frames.lock();
        assert_eq!(reply_frames.len(), 2);
        assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
        assert_eq!(reply_frames[1].msg_type.as_u16(), 303);
        let error_response = parse_forwarded_rpc_response(&reply_frames[1]);
        assert_eq!(error_response.correlation_id, request.correlation_id);
        assert_eq!(error_response.seq, 0);
        assert!(error_response.stream_end);
        assert_rpc_code_error(
            error_response.body.as_ref(),
            crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
            RPC_INVALID_SEQUENCE_ERROR,
        );

        let worker_frames = worker_frames.lock();
        assert_eq!(worker_frames.len(), 2);
        assert_eq!(worker_frames[0].msg_type.as_u16(), 302);
        assert_eq!(worker_frames[1].msg_type.as_u16(), 303);
        let worker_error = parse_forwarded_rpc_response(&worker_frames[1]);
        assert_eq!(worker_error.correlation_id, request.correlation_id);
        assert!(worker_error.stream_end);
        assert_rpc_code_error(
            worker_error.body.as_ref(),
            crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
            RPC_INVALID_SEQUENCE_ERROR,
        );
    }

    #[test]
    fn should_reject_duplicate_worker_response_chunk_given_rpc_sink() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
        let family = RouteFamily::new(1);
        let request_route = Route::new("rpc://bench/system/resource/operation");
        let request_addr = RouteAddress::new(family, request_route.clone());
        let request_source = session_inbox_address(family, 1);
        let worker_source = session_inbox_address(family, 42);
        let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
        let reply_sink = Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        });
        let worker_sink = Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        });
        router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
        router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
        {
            let mut state = sink.state.lock();
            state
                .ensure_route_state(&request_route)
                .register_worker(test_rpc_worker(family, &request_route, 42));
        }
        let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
        let (request_msg_type, request_payload) =
            crate::benchkit::extract_single_tlv_field(&request_frame);
        let request_ctx = FrameContext::new(
            1,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(request_msg_type),
            request_payload.clone(),
            family,
        );
        let request =
            match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
                .expect("parse rpc request")
            {
                crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
                other => panic!("expected rpc request, found {other:?}"),
            };
        let first_response_payload = crate::protocol::rpc_codec::encode_response_message(
            &crate::domains::rpc::protocol::RpcResponse::chunk(
                request.correlation_id,
                0,
                bytes::Bytes::from_static(b"part-0"),
                false,
            ),
        );
        let duplicate_response_payload = crate::protocol::rpc_codec::encode_response_message(
            &crate::domains::rpc::protocol::RpcResponse::chunk(
                request.correlation_id,
                0,
                bytes::Bytes::from_static(b"part-0-again"),
                false,
            ),
        );

        // Act
        sink.deliver(Envelope::from_route(
            request_source,
            request_addr.clone(),
            request_ctx,
        ))
        .expect("deliver request");
        sink.deliver(Envelope::from_route(
            worker_source.clone(),
            request_addr.clone(),
            FrameContext::new(
                42,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(first_response_payload),
                family,
            ),
        ))
        .expect("deliver first response chunk");
        sink.deliver(Envelope::from_route(
            worker_source,
            request_addr,
            FrameContext::new(
                42,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(duplicate_response_payload),
                family,
            ),
        ))
        .expect("deliver duplicate response chunk");

        // Assert
        assert_eq!(sink.pending_request_count(), 0);

        let reply_frames = reply_frames.lock();
        assert_eq!(reply_frames.len(), 3);
        assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
        let first_response = parse_forwarded_rpc_response(&reply_frames[1]);
        assert_eq!(first_response.correlation_id, request.correlation_id);
        assert_eq!(first_response.seq, 0);
        assert!(!first_response.stream_end);
        let terminal_error = parse_forwarded_rpc_response(&reply_frames[2]);
        assert_eq!(terminal_error.correlation_id, request.correlation_id);
        assert!(terminal_error.stream_end);
        assert_rpc_code_error(
            terminal_error.body.as_ref(),
            crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
            RPC_INVALID_SEQUENCE_ERROR,
        );

        let worker_frames = worker_frames.lock();
        assert_eq!(worker_frames.len(), 3);
        assert_eq!(worker_frames[0].msg_type.as_u16(), 302);
        assert_eq!(worker_frames[1].msg_type.as_u16(), 304);
        assert_eq!(worker_frames[2].msg_type.as_u16(), 303);
        let worker_error = parse_forwarded_rpc_response(&worker_frames[2]);
        assert_eq!(worker_error.correlation_id, request.correlation_id);
        assert!(worker_error.stream_end);
        assert_rpc_code_error(
            worker_error.body.as_ref(),
            crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
            RPC_INVALID_SEQUENCE_ERROR,
        );
    }
}
