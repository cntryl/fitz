use crate::protocol::frame_context::FrameContext;
use crate::runtime::routing::{session_inbox_address, Route, RouteAddress, RouteFamily};
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use chrono::Utc;
use fxhash::FxBuildHasher;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

type RpcFastMap<K, V> = HashMap<K, V, FxBuildHasher>;

const RPC_BACKPRESSURE_ERROR: &str = "RPC backpressure: too many pending requests";
const RPC_NO_WORKERS_ERROR: &str = "No workers registered for route";
const RPC_WORKER_NOT_FOUND_ERROR: &str = "Worker disconnected or unregistered";
const RPC_MAX_PENDING_REQUESTS: usize = 4096;
const RPC_ADMIN_SNAPSHOT_INTERVAL_US: u64 = 250_000;

fn parse_route_quad(route: &str) -> Option<(String, String, String, String)> {
    let path = route.split("://").nth(1).unwrap_or(route);
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if parts.len() < 4 {
        return None;
    }
    Some((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
        parts[3].to_string(),
    ))
}

#[derive(Clone)]
struct RpcWorker {
    addr: RouteAddress,
    inbox_addr: RouteAddress,
    session_id: u64,
}

#[derive(Clone)]
struct RpcPendingRequest {
    caller_session_id: u64,
    caller_inbox_addr: Option<RouteAddress>,
    worker_addr: RouteAddress,
    worker_session_id: u64,
}

struct RpcPendingDisconnectDelivery {
    correlation_id: uuid::Uuid,
    caller_session_id: u64,
    caller_inbox_addr: RouteAddress,
}

struct RpcPendingCleanupResult {
    detached_callers: usize,
    removed_pending: usize,
    pending_len: usize,
    disconnect_deliveries: Vec<RpcPendingDisconnectDelivery>,
}

struct RpcSessionCleanupResult {
    removed_workers: usize,
    detached_callers: usize,
    removed_pending: usize,
    pending_len: usize,
    disconnect_deliveries: Vec<RpcPendingDisconnectDelivery>,
}

struct RpcWorkerCleanupResult {
    removed_workers: usize,
    removed_pending: usize,
    pending_len: usize,
    disconnect_deliveries: Vec<RpcPendingDisconnectDelivery>,
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
}

impl RpcPendingTable {
    fn new() -> Self {
        Self {
            pending: HashMap::with_capacity_and_hasher(256, FxBuildHasher::default()),
        }
    }

    fn track_pending(
        &mut self,
        correlation_id: uuid::Uuid,
        caller_session_id: u64,
        caller_inbox_addr: RouteAddress,
        worker_addr: RouteAddress,
        worker_session_id: u64,
    ) -> usize {
        self.pending.insert(
            correlation_id,
            RpcPendingRequest {
                caller_session_id,
                caller_inbox_addr: Some(caller_inbox_addr),
                worker_addr,
                worker_session_id,
            },
        );
        self.pending.len()
    }

    fn pending_for_response(
        &mut self,
        correlation_id: &uuid::Uuid,
        stream_end: bool,
    ) -> Option<(RpcPendingRequest, usize, bool)> {
        let pending = self.pending.get(correlation_id).cloned()?;
        if stream_end {
            self.pending.remove(correlation_id);
            return Some((pending, self.pending.len(), true));
        }

        Some((pending, self.pending.len(), false))
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
                        disconnect_deliveries.push(RpcPendingDisconnectDelivery {
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
                    disconnect_deliveries.push(RpcPendingDisconnectDelivery {
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
            snapshot_dirty: AtomicBool::new(false),
            snapshot_syncing: AtomicBool::new(false),
            last_snapshot_elapsed_us: AtomicU64::new(0),
            snapshot_epoch: Instant::now(),
            metrics: None,
        }
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

    fn counter_inc(&self, name: &str) {
        if let Some(ref metrics) = self.metrics {
            metrics.counter_inc(name);
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
            self.counter_inc("rpc_cleanup_workers_removed_total");
        }
        if cleanup_result.detached_callers > 0 {
            self.counter_inc("rpc_cleanup_callers_detached_total");
        }
        if cleanup_result.removed_pending > 0 {
            self.counter_inc("rpc_cleanup_pending_removed_total");
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
            self.counter_inc("rpc_cleanup_workers_removed_total");
        }
        if cleanup_result.removed_pending > 0 {
            self.counter_inc("rpc_cleanup_pending_removed_total");
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

    fn forward_worker_disconnect_errors(
        &self,
        disconnect_deliveries: Vec<RpcPendingDisconnectDelivery>,
    ) {
        if disconnect_deliveries.is_empty() {
            return;
        }

        let mut error_body_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(96);
        let mut response_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(128);

        for disconnect in disconnect_deliveries {
            let error_body = crate::protocol::rpc_codec::encode_error_body_into(
                crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
                RPC_WORKER_NOT_FOUND_ERROR,
                &mut error_body_encoder,
            );
            let error_response = crate::domains::rpc::protocol::RpcResponse::single(
                disconnect.correlation_id,
                bytes::Bytes::from(error_body),
            );
            let encoded_response = crate::protocol::rpc_codec::encode_response_message_into(
                &error_response,
                &mut response_encoder,
            );
            let response_ctx = FrameContext::new(
                disconnect.caller_session_id,
                crate::protocol::frame::ChannelId::Rpc,
                crate::protocol::tlv::MessageType::new(303),
                bytes::Bytes::from(encoded_response),
                *disconnect.caller_inbox_addr.family(),
            );
            let response_envelope = Envelope::new(disconnect.caller_inbox_addr, response_ctx);

            if let Err(error) = self.router.route(response_envelope) {
                self.counter_inc("rpc_worker_disconnect_errors_dropped_total");
                tracing::warn!(
                    domain = "rpc",
                    correlation_id = %disconnect.correlation_id,
                    error = ?error,
                    "Failed to forward worker disconnect error to requester"
                );
            } else {
                self.counter_inc("rpc_worker_disconnect_errors_forwarded_total");
            }
        }
    }

    fn sync_admin_snapshot(&self) {
        let state = self.state.lock();
        let workers = state
            .routes
            .iter()
            .flat_map(|(route, route_state)| {
                route_state
                    .workers
                    .iter()
                    .filter_map(|worker| {
                        parse_route_quad(route.as_str()).map(
                            |(realm, _area, _resource, _operation)| crate::api::admin::RpcWorker {
                                session_id: worker.session_id.to_string(),
                                realm,
                                route: route.as_str().to_string(),
                                registered_at: Utc::now().to_rfc3339(),
                                requests_handled: 0,
                                average_latency_ms: 0.0,
                            },
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let pending = state
            .pending
            .pending
            .iter()
            .map(
                |(correlation_id, pending)| crate::api::admin::RpcPendingRequest {
                    correlation_id: correlation_id.to_string(),
                    route: format!("rpc://pending/session/{}", pending.caller_session_id),
                    submitted_at: Utc::now().to_rfc3339(),
                    age_seconds: 0,
                    worker_session_id: None,
                },
            )
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

    fn schedule_admin_snapshot(&self, force: bool) {
        self.snapshot_dirty.store(true, Ordering::Relaxed);
        self.maybe_sync_admin_snapshot(force);
    }

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
                route_state.register_worker(RpcWorker {
                    addr: worker_addr.clone(),
                    inbox_addr: worker_inbox_addr,
                    session_id: frame_ctx.session_id,
                });
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
                        let pending_track_start = Instant::now();
                        let pending_len = state.pending.track_pending(
                            req.correlation_id,
                            frame_ctx.session_id,
                            caller_inbox_addr,
                            worker.addr.clone(),
                            worker.session_id,
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
                    .pending_for_response(&resp.correlation_id, resp.stream_end);
                let pending_route_lookup_us =
                    pending_route_lookup_start.elapsed().as_micros() as u64;
                let mut state_changed = false;

                if let Some((caller_info, pending_len, removed_pending)) = caller_info {
                    let pending_lookup_us = pending_route_lookup_us;
                    if removed_pending {
                        self.histogram_observe_us("rpc_pending_route_remove_us", pending_lookup_us);
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
                } else {
                    let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                    drop(state);
                    self.histogram_observe_us(
                        "rpc_pending_route_lookup_us",
                        pending_route_lookup_us,
                    );
                    self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
                    self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);
                    self.counter_inc("rpc_responses_missing_pending_total");
                    tracing::warn!(
                        domain = "rpc",
                        correlation_id = %resp.correlation_id,
                        "No pending request for response"
                    );
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
        route_state.register_worker(RpcWorker {
            addr: RouteAddress::new(family, route.clone()),
            inbox_addr: session_inbox_address(family, 10),
            session_id: 10,
        });
        route_state.register_worker(RpcWorker {
            addr: RouteAddress::new(family, route),
            inbox_addr: session_inbox_address(family, 11),
            session_id: 11,
        });

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
        state.ensure_route_state(&route).register_worker(RpcWorker {
            addr: RouteAddress::new(family, route),
            inbox_addr: session_inbox_address(family, 10),
            session_id: 10,
        });

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
                .register_worker(RpcWorker {
                    addr: RouteAddress::new(family, request_route.clone()),
                    inbox_addr: session_inbox_address(family, 42),
                    session_id: 42,
                });
            for _ in 0..RPC_MAX_PENDING_REQUESTS {
                state.pending.track_pending(
                    uuid::Uuid::new_v4(),
                    7,
                    session_inbox_address(family, 7),
                    RouteAddress::new(family, request_route.clone()),
                    42,
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
                .register_worker(RpcWorker {
                    addr: request_addr.clone(),
                    inbox_addr: session_inbox_address(family, 42),
                    session_id: 42,
                });
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
                .register_worker(RpcWorker {
                    addr: request_addr.clone(),
                    inbox_addr: worker_source.clone(),
                    session_id: 42,
                });
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
        state.ensure_route_state(&route).register_worker(RpcWorker {
            addr: RouteAddress::new(family, route.clone()),
            inbox_addr: session_inbox_address(family, 42),
            session_id: 42,
        });
        let detached_correlation_id = uuid::Uuid::new_v4();
        state.pending.track_pending(
            detached_correlation_id,
            7,
            caller_inbox_addr.clone(),
            RouteAddress::new(family, route.clone()),
            42,
        );

        // Act
        let caller_cleanup = state.cleanup_session(7);

        // Assert
        assert_eq!(caller_cleanup.removed_workers, 0);
        assert_eq!(caller_cleanup.detached_callers, 1);
        assert_eq!(caller_cleanup.removed_pending, 0);
        assert_eq!(caller_cleanup.pending_len, 1);
        let (detached_pending, pending_len, removed) = state
            .pending
            .pending_for_response(&detached_correlation_id, false)
            .expect("detached pending should remain tracked");
        assert_eq!(detached_pending.caller_session_id, 7);
        assert_eq!(detached_pending.caller_inbox_addr, None);
        assert_eq!(
            detached_pending.worker_addr,
            RouteAddress::new(family, route)
        );
        assert_eq!(detached_pending.worker_session_id, 42);
        assert_eq!(pending_len, 1);
        assert!(!removed);
        assert_eq!(state.route_count(), 1);
    }

    #[test]
    fn should_remove_worker_entries_given_rpc_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let mut state = RpcState::new();
        let route = Route::new("rpc://bench/system/resource/operation");
        state.ensure_route_state(&route).register_worker(RpcWorker {
            addr: RouteAddress::new(family, route.clone()),
            inbox_addr: session_inbox_address(family, 42),
            session_id: 42,
        });

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
            99,
            session_inbox_address(family, 99),
            RouteAddress::new(family, worker_route),
            42,
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
            .register_worker(RpcWorker {
                addr: removed_worker_addr.clone(),
                inbox_addr: session_inbox_address(family, 42),
                session_id: 42,
            });
        state
            .ensure_route_state(&retained_route)
            .register_worker(RpcWorker {
                addr: retained_worker_addr.clone(),
                inbox_addr: session_inbox_address(family, 42),
                session_id: 42,
            });
        state.pending.track_pending(
            removed_correlation_id,
            99,
            session_inbox_address(family, 99),
            removed_worker_addr.clone(),
            42,
        );
        state.pending.track_pending(
            retained_correlation_id,
            100,
            session_inbox_address(family, 100),
            retained_worker_addr.clone(),
            42,
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
            state
                .pending
                .pending_for_response(&removed_correlation_id, false)
                .is_none(),
            "removed worker pending should no longer be tracked"
        );
        let (retained_pending, pending_len, removed) = state
            .pending
            .pending_for_response(&retained_correlation_id, false)
            .expect("retained worker pending should remain tracked");
        assert_eq!(retained_pending.worker_addr, retained_worker_addr);
        assert_eq!(pending_len, 1);
        assert!(!removed);
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
                .register_worker(RpcWorker {
                    addr: request_addr.clone(),
                    inbox_addr: worker_source.clone(),
                    session_id: 42,
                });
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
                .register_worker(RpcWorker {
                    addr: request_addr.clone(),
                    inbox_addr: worker_source,
                    session_id: 42,
                });
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
                .register_worker(RpcWorker {
                    addr: request_addr.clone(),
                    inbox_addr: worker_source.clone(),
                    session_id: 42,
                });
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
            42,
            caller_inbox_addr.clone(),
            worker_addr.clone(),
            77,
        );

        // Act
        let result = pending.pending_for_response(&correlation_id, true);

        // Assert
        assert_eq!(pending_len, 1);
        let (tracked, pending_len, removed) = result.expect("pending request should exist");
        assert_eq!(tracked.caller_session_id, 42);
        assert_eq!(tracked.caller_inbox_addr, Some(caller_inbox_addr));
        assert_eq!(tracked.worker_addr, worker_addr);
        assert_eq!(tracked.worker_session_id, 77);
        assert!(removed);
        assert_eq!(pending_len, 0);
        assert_eq!(pending.len(), 0);
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
            84,
            caller_inbox_addr.clone(),
            worker_addr.clone(),
            99,
        );

        // Act
        let result = pending.pending_for_response(&correlation_id, false);

        // Assert
        assert_eq!(pending_len, 1);
        let (tracked, pending_len, removed) = result.expect("pending request should exist");
        assert_eq!(tracked.caller_session_id, 84);
        assert_eq!(tracked.caller_inbox_addr, Some(caller_inbox_addr));
        assert_eq!(tracked.worker_addr, worker_addr);
        assert_eq!(tracked.worker_session_id, 99);
        assert!(!removed);
        assert_eq!(pending_len, 1);
        assert_eq!(pending.len(), 1);
    }
}
