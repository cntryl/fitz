//! Runtime statistics and observability

use crate::api::runtime_ingress::RuntimeIngress;
use crate::boot::domains::DomainHandles;
use crate::runtime::Router;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize};
use std::sync::Arc;
use std::time::Instant;

mod admin_queries;
mod core;
mod domain_stats;

const LIFECYCLE_RUNNING: u8 = 0;
const LIFECYCLE_DRAINING: u8 = 1;
const LIFECYCLE_SHUTTING_DOWN: u8 = 2;

const DEFAULT_DRAIN_GRACE_SECONDS: u64 = 25;
const DEFAULT_DRAIN_CLOSE_REASON: &str = "broker draining for redeploy";
const ADMIN_BLOCKING_EXECUTOR_CAPACITY: usize = 8;

/// An owned slot in the bounded API-edge blocking executor.
///
/// This deliberately uses synchronous atomics because the permit is also
/// stored in `Runtime`, which is shared with the synchronous core. The slot
/// is returned when the blocking task drops the permit.
pub(crate) struct AdminBlockingPermit {
    slots: Arc<AtomicUsize>,
}

impl Drop for AdminBlockingPermit {
    fn drop(&mut self) {
        self.slots
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerLifecycleState {
    Running,
    Draining,
    ShuttingDown,
}

impl BrokerLifecycleState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Draining => "draining",
            Self::ShuttingDown => "shutting_down",
        }
    }

    pub(crate) fn from_u8(value: u8) -> Self {
        match value {
            LIFECYCLE_DRAINING => Self::Draining,
            LIFECYCLE_SHUTTING_DOWN => Self::ShuttingDown,
            _ => Self::Running,
        }
    }
}

/// Runtime statistics and state accessor
///
/// Provides read-only access to runtime metrics for observability.
/// This structure is thread-safe and can be cloned cheaply (Arc-wrapped).
#[derive(Clone)]
pub struct Runtime {
    /// Message router (for route/subscription queries)
    pub(crate) router: Arc<Router>,

    /// Startup timestamp
    pub(crate) startup_time: Instant,

    /// Storage ready flag
    pub(crate) storage_ready: Arc<AtomicU64>,

    /// Domains initialized flag
    pub(crate) domains_ready: Arc<AtomicU64>,

    /// Auth configuration validated flag
    pub(crate) auth_config_ready: Arc<AtomicU64>,

    /// Startup complete flag
    pub(crate) startup_complete: Arc<AtomicU64>,

    /// Runtime lifecycle state.
    pub(crate) lifecycle_state: Arc<AtomicU8>,

    /// Drain grace period in seconds.
    pub(crate) drain_grace_seconds: Arc<AtomicU64>,

    /// Epoch millis when the current drain started; 0 means no drain has started.
    pub(crate) drain_started_epoch_ms: Arc<AtomicU64>,

    /// Epoch millis when the current drain grace expires; 0 means no drain has started.
    pub(crate) drain_deadline_epoch_ms: Arc<AtomicU64>,

    /// Human-readable server close reason used for planned drain shutdown.
    pub(crate) drain_close_reason: Arc<RwLock<String>>,

    /// Active connection count
    pub(crate) connection_count: Arc<AtomicUsize>,

    /// Active session count
    pub(crate) session_count: Arc<AtomicUsize>,

    /// Number of synchronous family actor shards selected at startup.
    pub(crate) family_actor_shards: Arc<AtomicUsize>,

    /// Provisioned-family affinity table used by runtime dispatch adapters.
    /// The pool is configured before transports start accepting work.
    pub(crate) family_actor_ingress: Arc<RwLock<Option<crate::runtime::FamilyActorIngress<()>>>>,

    /// Set when an unrecoverable production domain actor failure requires the
    /// boot loop to terminate with a non-zero result.
    pub(crate) fatal_domain_failure: Arc<AtomicBool>,

    /// Total messages received
    pub(crate) messages_received: Arc<AtomicU64>,

    /// Total messages sent
    pub(crate) messages_sent: Arc<AtomicU64>,

    /// Admin auth configuration
    pub(crate) admin_auth: Arc<crate::api::admin::auth::AdminAuth>,

    /// Passive admin read model used by REST handlers
    pub(crate) admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,

    /// Live ingress handle for session visibility
    pub(crate) ingress: Arc<RwLock<Option<Arc<RuntimeIngress>>>>,

    /// Domain sink handles for live admin stats
    pub(crate) domains: Arc<RwLock<Option<Arc<DomainHandles>>>>,

    /// Auth configuration used by admin/auth surfaces
    pub(crate) auth_config: Arc<RwLock<crate::auth::AuthConfig>>,

    /// Whether a validated external TLS terminator protects public browser traffic.
    pub(crate) assume_external_tls: Arc<AtomicBool>,

    /// Available slots for synchronous storage-backed admin work moved off
    /// Tokio workers.
    pub(crate) admin_blocking_slots: Arc<AtomicUsize>,
}
