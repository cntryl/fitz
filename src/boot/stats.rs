//! Runtime statistics and observability

use crate::boot::domains::DomainHandles;
use crate::runtime::Router;
use crate::session::manager::RuntimeIngress;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::Instant;

mod admin_queries;
mod core;
mod domain_stats;

/// Runtime statistics and state accessor
///
/// Provides read-only access to runtime metrics for observability.
/// This structure is thread-safe and can be cloned cheaply (Arc-wrapped).
#[derive(Clone)]
pub struct Runtime {
    /// Message router (for route/subscription queries)
    #[allow(dead_code)] // TODO: Use for querying domain stats
    pub(crate) router: Arc<Router>,

    /// Startup timestamp
    pub(crate) startup_time: Instant,

    /// Storage ready flag
    pub(crate) storage_ready: Arc<AtomicU64>,

    /// Domains initialized flag
    pub(crate) domains_ready: Arc<AtomicU64>,

    /// Startup complete flag
    pub(crate) startup_complete: Arc<AtomicU64>,

    /// Active connection count
    pub(crate) connection_count: Arc<AtomicUsize>,

    /// Active session count
    pub(crate) session_count: Arc<AtomicUsize>,

    /// Total messages received
    pub(crate) messages_received: Arc<AtomicU64>,

    /// Total messages sent
    pub(crate) messages_sent: Arc<AtomicU64>,

    /// Admin auth configuration
    pub(crate) admin_auth: Arc<crate::api::admin::auth::AdminAuth>,

    /// Passive admin read model used by REST handlers
    pub(crate) admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,

    /// Live ingress handle for session visibility
    pub(crate) ingress: Arc<RwLock<Option<Arc<RuntimeIngress>>>>,

    /// Domain sink handles for live admin stats
    pub(crate) domains: Arc<RwLock<Option<Arc<DomainHandles>>>>,

    /// Auth configuration used by admin/auth surfaces
    pub(crate) auth_config: Arc<RwLock<crate::auth::AuthConfig>>,
}
