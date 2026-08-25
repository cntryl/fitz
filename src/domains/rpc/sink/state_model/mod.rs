pub(super) use crate::domains::rpc::{RpcClientRequest, RpcClientResponseBody};
pub(super) type RpcDeliveryOutcome = (Option<RpcClientResponseBody>, Option<bool>, bool);
#[cfg(test)]
pub(super) use crate::dispatch::protocol::frame_context::FrameContext;
pub(super) use crate::runtime::routing::{session_inbox_address, Route, RouteAddress, RouteFamily};
pub(super) use crate::runtime::{
    DeliveryError, Envelope, FamilyActorPoolRuntime, MailboxSink, ManagedActor, Router,
};
pub(super) use chrono::{DateTime, Utc};
pub(super) use parking_lot::Mutex;
pub(super) use rustc_hash::FxBuildHasher;
pub(super) use std::cmp::Ordering as HeapOrdering;
pub(super) use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
pub(super) use std::sync::{Arc, Weak};
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
mod ready_queue;
mod registration_table;
mod requests;
mod route_state;
mod sink;
mod snapshot;
mod state;
mod worker;

#[cfg(test)]
pub(super) use constants::RPC_MSG_TYPE_RESPONSE;
pub(super) use constants::{
    RPC_ACTOR_REPLY_TIMEOUT, RPC_ADMIN_SNAPSHOT_INTERVAL_US, RPC_BACKPRESSURE_ERROR,
    RPC_CORRELATION_NOT_FOUND_ERROR, RPC_DEFAULT_REQUEST_TIMEOUT,
    RPC_DEFAULT_ROUTE_PENDING_CAPACITY, RPC_DUPLICATE_CORRELATION_ERROR,
    RPC_INVALID_SEQUENCE_ERROR, RPC_MAX_PENDING_REQUESTS, RPC_MAX_TIMEOUT_SWEEP_INTERVAL,
    RPC_MIN_TIMEOUT_SWEEP_INTERVAL, RPC_MSG_TYPE_REQUEST, RPC_NO_WORKERS_ERROR,
    RPC_RESPONSE_UNDELIVERABLE_ERROR, RPC_TIMEOUT_ERROR, RPC_WORKER_NOT_FOUND_ERROR,
    RPC_WRONG_WORKER_ERROR,
};
pub(super) use expiration::{rpc_timeout_sweep_interval, ExpiringPendingRequest};
pub(super) use pending_table::{RpcPendingResponseDisposition, RpcPendingTable};
pub(super) use ready_queue::RouteReadyQueue;
pub(super) use registration_table::RegistrationTable;
pub(super) use requests::{
    RpcPendingCleanupResult, RpcPendingDispatchInfo, RpcPendingErrorDelivery, RpcPendingRequest,
    RpcPendingRequestInit, RpcPendingTimeoutResult, RpcQueuedDispatch, RpcQueuedRequest,
    RpcRequestDispatch, RpcRequestRejection, RpcSessionCleanupResult, RpcWorkerCleanupResult,
};
pub(super) use route_state::RpcRouteState;
pub use sink::RpcDomainSink;
pub(super) use sink::{
    RpcDomainActor, RpcDomainCommand, RpcDomainCore, RpcDomainRuntime, RpcLiveCounts,
};
pub(super) use snapshot::rpc_admin_snapshot_due;
#[cfg(test)]
pub(super) use state::RpcDispatchState;
pub(super) use state::{RpcRequestState, RpcResponseState, RpcState, RpcWorkerRegistration};
pub(super) use worker::{RpcRegistrationId, RpcWorker, RpcWorkerDispatch, RpcWorkerKey};
