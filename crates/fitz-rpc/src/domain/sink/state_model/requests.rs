use super::{DateTime, Instant, Route, RouteAddress, RpcRegistrationId, RpcWorkerDispatch, Utc};
use crate::wire::rpc_codec::RpcCancellationReason;

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
#[derive(Debug, Clone)]
pub(in crate::domain::sink) struct RpcPendingRequest {
    pub(in crate::domain::sink) dispatch_info: RpcPendingDispatchInfo,
    pub(in crate::domain::sink) worker_addr: RouteAddress,
    pub(in crate::domain::sink) worker_session_id: u64,
    pub(in crate::domain::sink) next_expected_seq: u64,
    /// Consecutive failures delivering the current chunk to the caller.
    pub(in crate::domain::sink) delivery_retries: u32,
    pub(in crate::domain::sink) submitted_at: DateTime<Utc>,
    pub(in crate::domain::sink) expires_at: Instant,
    pub(in crate::domain::sink) supports_cancellation: bool,
    pub(in crate::domain::sink) cancelled: bool,
    pub(in crate::domain::sink) close_requested: bool,
    /// Worker frame-loop busy time already credited to the cancellation grace.
    pub(in crate::domain::sink) grace_busy_baseline: Option<std::time::Duration>,
    /// Total busy time credited to the cancellation grace, capped by
    /// `RPC_MAX_CANCELLATION_GRACE_DEFERRAL` for this cancellation.
    pub(in crate::domain::sink) grace_deferred: std::time::Duration,
}

#[derive(Clone, Debug)]
pub(in crate::domain::sink) struct RpcPendingDispatchInfo {
    /// Opaque per-dispatch identity echoed by negotiated workers.
    pub(in crate::domain::sink) worker_correlation_id: uuid::Uuid,
    pub(in crate::domain::sink) family: fitz_core::runtime::routing::RouteFamily,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::domain::sink) route: Route,
    pub(in crate::domain::sink) caller_session_id: u64,
    pub(in crate::domain::sink) caller_inbox_addr: Option<RouteAddress>,
    pub(in crate::domain::sink) registration_id: RpcRegistrationId,
    pub(in crate::domain::sink) submitted_at_instant: Instant,
}

pub(in crate::domain::sink) struct RpcPendingRequestInit {
    pub(in crate::domain::sink) route: Route,
    pub(in crate::domain::sink) caller_session_id: u64,
    pub(in crate::domain::sink) caller_inbox_addr: RouteAddress,
    pub(in crate::domain::sink) registration_addr: RouteAddress,
    pub(in crate::domain::sink) registration_session_id: u64,
    pub(in crate::domain::sink) registration_id: RpcRegistrationId,
    pub(in crate::domain::sink) submitted_at: DateTime<Utc>,
    pub(in crate::domain::sink) submitted_at_instant: Instant,
    pub(in crate::domain::sink) expires_at: Instant,
}

impl RpcPendingRequest {
    pub(in crate::domain::sink) fn new(init: RpcPendingRequestInit) -> Self {
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
                worker_correlation_id: uuid::Uuid::nil(),
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
            supports_cancellation: false,
            cancelled: false,
            close_requested: false,
            grace_busy_baseline: None,
            grace_deferred: std::time::Duration::ZERO,
        }
    }

    pub(in crate::domain::sink) fn from_dispatch(
        req: &crate::domain::protocol::RpcRequest,
        caller_session_id: u64,
        caller_inbox_addr: RouteAddress,
        registration: &RpcWorkerDispatch,
        expires_at: Instant,
    ) -> Self {
        let submitted_at_instant = Instant::now();
        let mut pending = Self::new(RpcPendingRequestInit {
            route: req.route.clone(),
            caller_session_id,
            caller_inbox_addr,
            registration_addr: registration.addr.clone(),
            registration_session_id: registration.session_id,
            registration_id: registration.registration_id,
            submitted_at: Utc::now(),
            submitted_at_instant,
            expires_at,
        });
        pending.supports_cancellation = registration.supports_cancellation;
        pending
    }

    pub(in crate::domain::sink) fn dispatch_info(&self) -> RpcPendingDispatchInfo {
        self.dispatch_info.clone()
    }

    pub(in crate::domain::sink) fn into_dispatch_info(self) -> RpcPendingDispatchInfo {
        self.dispatch_info
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domain::sink) fn submitted_at_rfc3339(&self) -> String {
        self.submitted_at
            .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domain::sink) fn age_seconds(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.dispatch_info.submitted_at_instant)
            .as_secs()
    }
}

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
#[derive(Debug, Clone)]
pub(in crate::domain::sink) struct RpcQueuedRequest {
    pub(in crate::domain::sink) request: crate::domain::protocol::RpcRequest,
    pub(in crate::domain::sink) caller_session_id: u64,
    pub(in crate::domain::sink) caller_inbox_addr: RouteAddress,
    pub(in crate::domain::sink) submitted_at: DateTime<Utc>,
    pub(in crate::domain::sink) submitted_at_instant: Instant,
    pub(in crate::domain::sink) expires_at: Instant,
}

impl RpcQueuedRequest {
    pub(in crate::domain::sink) fn from_request(
        request: crate::domain::protocol::RpcRequest,
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
    pub(in crate::domain::sink) fn submitted_at_rfc3339(&self) -> String {
        self.submitted_at
            .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domain::sink) fn age_seconds(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.submitted_at_instant)
            .as_secs()
    }
}

pub(in crate::domain::sink) struct RpcPendingErrorDelivery {
    pub(in crate::domain::sink) correlation_id: uuid::Uuid,
    pub(in crate::domain::sink) caller_session_id: u64,
    pub(in crate::domain::sink) caller_inbox_addr: RouteAddress,
}

pub(in crate::domain::sink) struct RpcPendingCleanupResult {
    pub(in crate::domain::sink) detached_callers: usize,
    pub(in crate::domain::sink) removed_pending: usize,
    pub(in crate::domain::sink) disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
    pub(in crate::domain::sink) cancellations: Vec<RpcWorkerCancellation>,
}

#[derive(Default)]
pub(in crate::domain::sink) struct RpcSessionCleanupResult {
    pub(in crate::domain::sink) removed_registrations: usize,
    pub(in crate::domain::sink) detached_callers: usize,
    pub(in crate::domain::sink) removed_pending: usize,
    pub(in crate::domain::sink) pending_len: usize,
    pub(in crate::domain::sink) disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
    pub(in crate::domain::sink) cancellations: Vec<RpcWorkerCancellation>,
}

#[derive(Default)]
pub(in crate::domain::sink) struct RpcWorkerCleanupResult {
    pub(in crate::domain::sink) removed_registrations: usize,
    pub(in crate::domain::sink) removed_pending: usize,
    pub(in crate::domain::sink) pending_len: usize,
    pub(in crate::domain::sink) disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
}

pub(in crate::domain::sink) struct RpcPendingTimeoutResult {
    pub(in crate::domain::sink) removed_pending: usize,
    pub(in crate::domain::sink) pending_len: usize,
    pub(in crate::domain::sink) closed_caller_drops: usize,
    pub(in crate::domain::sink) timeout_deliveries: Vec<RpcPendingErrorDelivery>,
    pub(in crate::domain::sink) timed_out_requests: usize,
    pub(in crate::domain::sink) cancellations: Vec<RpcWorkerCancellation>,
    pub(in crate::domain::sink) close_worker_sessions:
        Vec<(fitz_core::runtime::routing::RouteFamily, u64)>,
}

pub(in crate::domain::sink) struct RpcWorkerCancellation {
    pub(in crate::domain::sink) family: fitz_core::runtime::routing::RouteFamily,
    pub(in crate::domain::sink) correlation_id: uuid::Uuid,
    pub(in crate::domain::sink) worker_session_id: u64,
    pub(in crate::domain::sink) reason: RpcCancellationReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::domain::sink) enum RpcCancellationDisposition {
    QueuedRemoved,
    Dispatched {
        worker_session_id: u64,
        supports_cancellation: bool,
    },
    AlreadyCancelled,
    UnauthorizedOrUnknown,
}

pub(in crate::domain::sink) enum RpcCancellationAckDisposition {
    /// Boxed so the grace accounting fields do not inflate every disposition.
    Acknowledged(Box<RpcPendingRequest>),
    Rejected,
}

pub(in crate::domain::sink) struct RpcQueuedDispatch {
    pub(in crate::domain::sink) request: crate::domain::protocol::RpcRequest,
    pub(in crate::domain::sink) registration: RpcWorkerDispatch,
    pub(in crate::domain::sink) live_request_count: usize,
    pub(in crate::domain::sink) expires_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub(in crate::domain::sink) enum RpcRequestRejection {
    Duplicate,
    NoWorkers,
    GlobalCapacityFull,
    RouteCapacityFull,
    Expired,
}

pub(in crate::domain::sink) enum RpcRequestDispatch {
    Rejected {
        request: crate::domain::protocol::RpcRequest,
        reason: RpcRequestRejection,
    },
    Queued {
        route: Route,
        correlation_id: uuid::Uuid,
        live_request_count: usize,
    },
    Immediate {
        request: crate::domain::protocol::RpcRequest,
        registration: RpcWorkerDispatch,
        live_request_count: usize,
        expires_at: Instant,
    },
}
