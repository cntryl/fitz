use super::{DateTime, Instant, Route, RouteAddress, RpcRegistrationId, RpcWorkerDispatch, Utc};

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
#[derive(Debug, Clone)]
pub(in crate::domains::rpc::sink) struct RpcPendingRequest {
    pub(in crate::domains::rpc::sink) dispatch_info: RpcPendingDispatchInfo,
    pub(in crate::domains::rpc::sink) worker_addr: RouteAddress,
    pub(in crate::domains::rpc::sink) worker_session_id: u64,
    pub(in crate::domains::rpc::sink) next_expected_seq: u64,
    /// Consecutive failures delivering the current chunk to the caller.
    pub(in crate::domains::rpc::sink) delivery_retries: u32,
    pub(in crate::domains::rpc::sink) submitted_at: DateTime<Utc>,
    pub(in crate::domains::rpc::sink) expires_at: Instant,
}

#[derive(Clone, Debug)]
pub(in crate::domains::rpc::sink) struct RpcPendingDispatchInfo {
    pub(in crate::domains::rpc::sink) family: crate::runtime::routing::RouteFamily,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::domains::rpc::sink) route: Route,
    pub(in crate::domains::rpc::sink) caller_session_id: u64,
    pub(in crate::domains::rpc::sink) caller_inbox_addr: Option<RouteAddress>,
    pub(in crate::domains::rpc::sink) registration_id: RpcRegistrationId,
    pub(in crate::domains::rpc::sink) submitted_at_instant: Instant,
}

pub(in crate::domains::rpc::sink) struct RpcPendingRequestInit {
    pub(in crate::domains::rpc::sink) route: Route,
    pub(in crate::domains::rpc::sink) caller_session_id: u64,
    pub(in crate::domains::rpc::sink) caller_inbox_addr: RouteAddress,
    pub(in crate::domains::rpc::sink) registration_addr: RouteAddress,
    pub(in crate::domains::rpc::sink) registration_session_id: u64,
    pub(in crate::domains::rpc::sink) registration_id: RpcRegistrationId,
    pub(in crate::domains::rpc::sink) submitted_at: DateTime<Utc>,
    pub(in crate::domains::rpc::sink) submitted_at_instant: Instant,
    pub(in crate::domains::rpc::sink) expires_at: Instant,
}

impl RpcPendingRequest {
    pub(in crate::domains::rpc::sink) fn new(init: RpcPendingRequestInit) -> Self {
        let RpcPendingRequestInit {
            route,
            caller_session_id,
            caller_inbox_addr,
            registration_addr,
            registration_session_id,
            registration_id,
            submitted_at,
            submitted_at_instant,
            expires_at,
        } = init;

        Self {
            dispatch_info: RpcPendingDispatchInfo {
                family: *registration_addr.family(),
                route,
                caller_session_id,
                caller_inbox_addr: Some(caller_inbox_addr),
                registration_id,
                submitted_at_instant,
            },
            worker_addr: registration_addr,
            worker_session_id: registration_session_id,
            next_expected_seq: 0,
            delivery_retries: 0,
            submitted_at,
            expires_at,
        }
    }

    pub(in crate::domains::rpc::sink) fn from_dispatch(
        req: &crate::domains::rpc::protocol::RpcRequest,
        caller_session_id: u64,
        caller_inbox_addr: RouteAddress,
        registration: &RpcWorkerDispatch,
        expires_at: Instant,
    ) -> Self {
        let submitted_at_instant = Instant::now();
        Self::new(RpcPendingRequestInit {
            route: req.route.clone(),
            caller_session_id,
            caller_inbox_addr,
            registration_addr: registration.addr.clone(),
            registration_session_id: registration.session_id,
            registration_id: registration.registration_id,
            submitted_at: Utc::now(),
            submitted_at_instant,
            expires_at,
        })
    }

    pub(in crate::domains::rpc::sink) fn dispatch_info(&self) -> RpcPendingDispatchInfo {
        self.dispatch_info.clone()
    }

    pub(in crate::domains::rpc::sink) fn into_dispatch_info(self) -> RpcPendingDispatchInfo {
        self.dispatch_info
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) fn submitted_at_rfc3339(&self) -> String {
        self.submitted_at
            .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) fn age_seconds(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.dispatch_info.submitted_at_instant)
            .as_secs()
    }
}

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
#[derive(Debug, Clone)]
pub(in crate::domains::rpc::sink) struct RpcQueuedRequest {
    pub(in crate::domains::rpc::sink) request: crate::domains::rpc::protocol::RpcRequest,
    pub(in crate::domains::rpc::sink) caller_session_id: u64,
    pub(in crate::domains::rpc::sink) caller_inbox_addr: RouteAddress,
    pub(in crate::domains::rpc::sink) submitted_at: DateTime<Utc>,
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
            submitted_at: Utc::now(),
            submitted_at_instant: Instant::now(),
            expires_at,
        }
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) fn submitted_at_rfc3339(&self) -> String {
        self.submitted_at
            .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
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
    pub(in crate::domains::rpc::sink) removed_registrations: usize,
    pub(in crate::domains::rpc::sink) detached_callers: usize,
    pub(in crate::domains::rpc::sink) removed_pending: usize,
    pub(in crate::domains::rpc::sink) pending_len: usize,
    pub(in crate::domains::rpc::sink) disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
}

#[derive(Default)]
pub(in crate::domains::rpc::sink) struct RpcWorkerCleanupResult {
    pub(in crate::domains::rpc::sink) removed_registrations: usize,
    pub(in crate::domains::rpc::sink) removed_pending: usize,
    pub(in crate::domains::rpc::sink) pending_len: usize,
    pub(in crate::domains::rpc::sink) disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
}

pub(in crate::domains::rpc::sink) struct RpcPendingTimeoutResult {
    pub(in crate::domains::rpc::sink) removed_pending: usize,
    pub(in crate::domains::rpc::sink) pending_len: usize,
    pub(in crate::domains::rpc::sink) closed_caller_drops: usize,
    pub(in crate::domains::rpc::sink) timeout_deliveries: Vec<RpcPendingErrorDelivery>,
}

pub(in crate::domains::rpc::sink) struct RpcQueuedDispatch {
    pub(in crate::domains::rpc::sink) request: crate::domains::rpc::protocol::RpcRequest,
    pub(in crate::domains::rpc::sink) registration: RpcWorkerDispatch,
    pub(in crate::domains::rpc::sink) live_request_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub(in crate::domains::rpc::sink) enum RpcRequestRejection {
    Duplicate,
    NoWorkers,
    GlobalCapacityFull,
    RouteCapacityFull,
}

pub(in crate::domains::rpc::sink) enum RpcRequestDispatch {
    Rejected {
        request: crate::domains::rpc::protocol::RpcRequest,
        reason: RpcRequestRejection,
    },
    Queued {
        route: Route,
        correlation_id: uuid::Uuid,
        live_request_count: usize,
    },
    Immediate {
        request: crate::domains::rpc::protocol::RpcRequest,
        registration: RpcWorkerDispatch,
        live_request_count: usize,
    },
}
