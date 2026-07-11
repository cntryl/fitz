pub(super) use crate::domains::rpc::{RpcClientRequest, RpcClientResponseBody};
pub(super) type RpcDeliveryOutcome = (Option<RpcClientResponseBody>, Option<bool>, bool);
#[cfg(test)]
pub(super) use crate::protocol::frame_context::FrameContext;
pub(super) use crate::runtime::routing::{
    route_quad, session_inbox_address, Route, RouteAddress, RouteFamily,
};
pub(super) use crate::runtime::{DeliveryError, Envelope, MailboxSink, ManagedActor, Router};
pub(super) use chrono::{DateTime, Utc};
pub(super) use fxhash::FxBuildHasher;
pub(super) use parking_lot::Mutex;
pub(super) use std::cmp::Ordering as HeapOrdering;
pub(super) use std::collections::{BinaryHeap, HashMap, VecDeque};
pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
pub(super) use std::sync::Arc;
pub(super) use std::time::{Duration, Instant};

pub(super) type RpcFastMap<K, V> = HashMap<K, V, FxBuildHasher>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct RpcCorrelationKey {
    pub(super) family: RouteFamily,
    pub(super) correlation_id: uuid::Uuid,
}

mod constants;
mod expiration;
mod pending_table;
mod requests;
mod route_state;
mod sink;
mod snapshot;
mod state;
mod worker;

pub(super) use constants::{
    RPC_ADMIN_SNAPSHOT_INTERVAL_US, RPC_BACKPRESSURE_ERROR, RPC_CORRELATION_NOT_FOUND_ERROR,
    RPC_DEFAULT_REQUEST_TIMEOUT, RPC_DEFAULT_ROUTE_PENDING_CAPACITY,
    RPC_DUPLICATE_CORRELATION_ERROR, RPC_INVALID_SEQUENCE_ERROR, RPC_MAX_PENDING_REQUESTS,
    RPC_MAX_TIMEOUT_SWEEP_INTERVAL, RPC_MIN_TIMEOUT_SWEEP_INTERVAL, RPC_NO_WORKERS_ERROR,
    RPC_TIMEOUT_ERROR, RPC_WORKER_NOT_FOUND_ERROR, RPC_WRONG_WORKER_ERROR,
};
pub(super) use expiration::{rpc_timeout_sweep_interval, ExpiringPendingRequest};
pub(super) use pending_table::{RpcPendingResponseDisposition, RpcPendingTable};
pub(super) use requests::{
    RpcPendingCleanupResult, RpcPendingDispatchInfo, RpcPendingErrorDelivery, RpcPendingRequest,
    RpcPendingRequestInit, RpcPendingTimeoutResult, RpcQueuedDispatch, RpcQueuedRequest,
    RpcRequestDispatch, RpcSessionCleanupResult, RpcWorkerCleanupResult,
};
pub(super) use route_state::RpcRouteState;
pub use sink::RpcDomainSink;
pub(super) use sink::{
    RpcDomainActor, RpcDomainCommand, RpcDomainCore, RpcDomainRuntime, RpcLiveCounts,
};
pub(super) use snapshot::rpc_admin_snapshot_due;
pub(super) use state::RpcState;
pub(super) use worker::{RpcWorker, RpcWorkerDispatch, RpcWorkerKey};
