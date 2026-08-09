//! Boot result and configuration types

use crate::api::ingress::IngressConfig;
use crate::api::runtime_ingress::RuntimeIngress;
use crate::runtime::Router;
use std::sync::Arc;
use tracing::info;

mod config;

pub(crate) use config::warn_defaulted_fast_queue_policy;
pub(crate) use config::LocalListenerExposure;
pub use config::{
    BootConfig, CloudDurabilityMode, CloudStorageConfig, QueueWritePolicy, QueueWritePolicySource,
    StorageMemtableConfig, StorageMode,
};

pub type BootResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Type alias for the complex runtime initialization return type.
type RuntimeComponents = (
    Arc<Router>,
    Arc<RuntimeIngress>,
    IngressConfig,
    crate::boot::Runtime,
);

/// Initialize runtime infrastructure
///
/// Creates:
/// - `Router` for message delivery
/// - `RuntimeIngress` for session management
/// - `IngressConfig` for transport configuration
/// - runtime stats tracker for observability
///
/// # Errors
///
/// Returns an error when boot configuration validation fails before runtime
/// components are constructed.
pub fn init(config: &BootConfig) -> BootResult<RuntimeComponents> {
    info!("Initializing runtime infrastructure");
    config.validate()?;

    // Create runtime components
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    // Attach router to ingress so frames can be dispatched into domains
    let ingress = Arc::new(
        RuntimeIngress::new(config.auth_required)
            .with_router(router.clone())
            .with_admin_read_model(admin_read_model.clone())
            .with_auth_config(config.auth_config.clone())
            .with_auth_claims_config(config.auth_claims_config.clone())
            .with_route_family_resolver(config.route_family_resolver.clone())
            .with_route_families(&config.route_families),
    );

    let ingress_config = IngressConfig::default()
        .with_frame_size(config.max_frame_size)
        .with_max_connections(config.max_connections)
        .with_channel_capacity(config.channel_capacity);

    // Create runtime stats tracker
    let runtime = crate::boot::Runtime::with_admin_read_model(router.clone(), admin_read_model);
    runtime.configure_route_families(&config.route_families);
    runtime.attach_ingress(ingress.clone());
    runtime.attach_auth_config(config.auth_config.clone());
    runtime.set_assume_external_tls(config.assume_external_tls);
    runtime.configure_drain(
        config.drain_grace_seconds,
        config.drain_close_reason.clone(),
    );

    info!("Runtime initialized");

    Ok((router, ingress, ingress_config, runtime))
}
