//! Boot result and configuration types

use crate::api::ingress::IngressConfig;
use crate::api::runtime_ingress::RuntimeIngress;
use crate::runtime::Router;
use std::sync::Arc;
use tracing::info;

mod config;

#[cfg(any(test, feature = "test-support"))]
pub(crate) use config::LocalListenerExposure;
pub use config::{
    BootConfig, CloudDurabilityMode, CloudStorageConfig, StorageMemtableConfig, StorageMode,
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
    Ok(init_validated(config))
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn init_with_test_route_families(
    config: &BootConfig,
    storage_config: &BootConfig,
) -> BootResult<RuntimeComponents> {
    // Validate the runtime's ordinary settings with the dense storage IDs.
    // Then validate the sparse actor set and its authorization mappings.
    let mut validation_config = config.clone();
    validation_config
        .route_families
        .clone_from(&storage_config.route_families);
    validation_config.validate()?;

    let families = &config.route_families;
    if families.is_empty()
        || families.windows(2).any(|pair| pair[0] >= pair[1])
        || families
            .iter()
            .any(|family| storage_config.route_families.binary_search(family).is_err())
    {
        return Err(
            "test route families must be a sorted, unique subset of storage families".into(),
        );
    }
    config
        .route_family_resolver
        .validate(families, config.auth_required)?;

    Ok(init_validated(config))
}

fn init_validated(config: &BootConfig) -> RuntimeComponents {
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
            .with_route_families(&config.route_families)
            .with_max_message_bytes(u32::try_from(config.max_frame_size).unwrap_or(u32::MAX)),
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

    (router, ingress, ingress_config, runtime)
}
