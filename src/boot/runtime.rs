//! Boot result and configuration types

use crate::api::ingress::IngressConfig;
use crate::runtime::Router;
use crate::session::manager::RuntimeIngress;
use std::sync::Arc;
use tracing::info;

mod config;

pub use config::{BootConfig, StorageMode};

pub type BootResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Type alias for the complex runtime initialization return type
type RuntimeComponents = (
    Arc<Router>,
    Arc<RuntimeIngress>,
    IngressConfig,
    crate::runtime::Scheduler,
    crate::boot::Runtime,
);

/// Initialize runtime infrastructure
///
/// Creates:
/// - Router for message delivery
/// - RuntimeIngress for session management
/// - IngressConfig for transport configuration
/// - Scheduler for actor execution
/// - Runtime stats tracker for observability
pub fn init(
    config: &BootConfig,
    store: &Arc<cntryl_midge::Engine>,
) -> BootResult<RuntimeComponents> {
    info!("Initializing runtime infrastructure");
    config.validate()?;

    // Create runtime components
    let router = Arc::new(Router::new());
    let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
    // Attach router to ingress so frames can be dispatched into domains
    let ingress = Arc::new(
        RuntimeIngress::new(config.auth_required)
            .with_router(router.clone())
            .with_admin_read_model(admin_read_model.clone())
            .with_auth_config(config.auth_config.clone())
            .with_store(store.clone()),
    );

    let ingress_config = IngressConfig::default()
        .with_frame_size(config.max_frame_size)
        .with_channel_capacity(config.channel_capacity);

    // Create scheduler
    let num_workers = num_cpus::get();
    let scheduler = crate::runtime::Scheduler::new(num_workers);

    // Create runtime stats tracker
    let runtime = crate::boot::Runtime::with_admin_read_model(router.clone(), admin_read_model);
    runtime.attach_ingress(ingress.clone());
    runtime.attach_auth_config(config.auth_config.clone());

    info!("Runtime initialized with {} worker threads", num_workers);

    Ok((router, ingress, ingress_config, scheduler, runtime))
}
