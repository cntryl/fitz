use super::{Instant, Route, RouteAddress, RpcWorker, Utc};

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
#[derive(Debug, Clone)]
pub(in crate::domains::rpc::sink) struct RpcPendingRequest {
    pub(in crate::domains::rpc::sink) route: Route,
    pub(in crate::domains::rpc::sink) caller_session_id: u64,
    pub(in crate::domains::rpc::sink) caller_inbox_addr: Option<RouteAddress>,
    pub(in crate::domains::rpc::sink) worker_addr: RouteAddress,
    pub(in crate::domains::rpc::sink) worker_session_id: u64,
    pub(in crate::domains::rpc::sink) next_expected_seq: u64,
    pub(in crate::domains::rpc::sink) submitted_at: String,
    pub(in crate::domains::rpc::sink) submitted_at_instant: Instant,
    pub(in crate::domains::rpc::sink) expires_at: Instant,
}

pub(in crate::domains::rpc::sink) struct RpcPendingRequestInit {
    pub(in crate::domains::rpc::sink) route: Route,
    pub(in crate::domains::rpc::sink) caller_session_id: u64,
    pub(in crate::domains::rpc::sink) caller_inbox_addr: RouteAddress,
    pub(in crate::domains::rpc::sink) worker_addr: RouteAddress,
    pub(in crate::domains::rpc::sink) worker_session_id: u64,
    pub(in crate::domains::rpc::sink) submitted_at: String,
    pub(in crate::domains::rpc::sink) submitted_at_instant: Instant,
    pub(in crate::domains::rpc::sink) expires_at: Instant,
}

impl RpcPendingRequest {
    pub(in crate::domains::rpc::sink) fn new(init: RpcPendingRequestInit) -> Self {
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

    pub(in crate::domains::rpc::sink) fn from_dispatch(
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

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) fn age_seconds(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.submitted_at_instant)
            .as_secs()
    }
}

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
#[derive(Debug, Clone)]
pub(in crate::domains::rpc::sink) struct RpcQueuedRequest {
    pub(in crate::domains::rpc::sink) request: crate::domains::rpc::protocol::RpcRequest,
    pub(in crate::domains::rpc::sink) caller_session_id: u64,
    pub(in crate::domains::rpc::sink) caller_inbox_addr: RouteAddress,
    pub(in crate::domains::rpc::sink) submitted_at: String,
    pub(in crate::domains::rpc::sink) submitted_at_instant: Instant,
    pub(in crate::domains::rpc::sink) expires_at: Instant,
}

impl RpcQueuedRequest {
    pub(in crate::domains::rpc::sink) fn from_request(
        request: crate::domains::rpc::protocol::RpcRequest,
        caller_session_id: u64,
        caller_inbox_addr: RouteAddress,
        expires_at: Instant,
    ) -> Self {
        Self {
            request,
            caller_session_id,
            caller_inbox_addr,
            submitted_at: Utc::now().to_rfc3339(),
            submitted_at_instant: Instant::now(),
            expires_at,
        }
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) fn age_seconds(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.submitted_at_instant)
            .as_secs()
    }
}

pub(in crate::domains::rpc::sink) struct RpcPendingErrorDelivery {
    pub(in crate::domains::rpc::sink) correlation_id: uuid::Uuid,
    pub(in crate::domains::rpc::sink) caller_session_id: u64,
    pub(in crate::domains::rpc::sink) caller_inbox_addr: RouteAddress,
}

pub(in crate::domains::rpc::sink) struct RpcPendingCleanupResult {
    pub(in crate::domains::rpc::sink) detached_callers: usize,
    pub(in crate::domains::rpc::sink) removed_pending: usize,
    pub(in crate::domains::rpc::sink) disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
}

#[derive(Default)]
pub(in crate::domains::rpc::sink) struct RpcSessionCleanupResult {
    pub(in crate::domains::rpc::sink) removed_workers: usize,
    pub(in crate::domains::rpc::sink) detached_callers: usize,
    pub(in crate::domains::rpc::sink) removed_pending: usize,
    pub(in crate::domains::rpc::sink) pending_len: usize,
    pub(in crate::domains::rpc::sink) disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
}

#[derive(Default)]
pub(in crate::domains::rpc::sink) struct RpcWorkerCleanupResult {
    pub(in crate::domains::rpc::sink) removed_workers: usize,
    pub(in crate::domains::rpc::sink) removed_pending: usize,
    pub(in crate::domains::rpc::sink) pending_len: usize,
    pub(in crate::domains::rpc::sink) disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
}

pub(in crate::domains::rpc::sink) struct RpcPendingTimeoutResult {
    pub(in crate::domains::rpc::sink) removed_pending: usize,
    pub(in crate::domains::rpc::sink) pending_len: usize,
    pub(in crate::domains::rpc::sink) timeout_deliveries: Vec<RpcPendingErrorDelivery>,
}

pub(in crate::domains::rpc::sink) struct RpcQueuedDispatch {
    pub(in crate::domains::rpc::sink) request: crate::domains::rpc::protocol::RpcRequest,
    pub(in crate::domains::rpc::sink) worker: RpcWorker,
    pub(in crate::domains::rpc::sink) live_request_count: usize,
}
