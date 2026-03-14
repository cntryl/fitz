//! Boot module: Broker initialization and startup
//!
//! Responsible for:
//! - Opening storage
//! - Creating runtime infrastructure
//! - Spawning domain actors
//! - Starting transport listeners
//! - Coordinating graceful shutdown
//!
//! Each submodule is independently unit-testable.

pub mod domains;
pub mod handlers;
pub mod observability;
pub mod runtime;
pub mod stats;
pub mod storage;

pub use runtime::{BootConfig, BootResult};
pub use stats::Runtime;

/// Complete broker boot sequence
///
/// # Steps
/// 0. Initialize observability (tracing, metrics, OTEL)
/// 1. Open storage
/// 2. Create runtime (router, ingress, scheduler)
/// 3. Register domain actors
/// 4. Spawn transport listeners (TCP, HTTP/WS)
/// 5. Wait for Ctrl+C
/// 6. Graceful shutdown
pub async fn boot(config: BootConfig) -> BootResult<()> {
    // Step 0: Initialize observability (tracing, metrics, OTEL)
    let _metrics = observability::init_observability()?;

    tracing::info!("Starting Fitz broker");

    // Step 1: Open storage
    let store = storage::init(&config).await?;
    tracing::info!("Storage initialized");

    // Step 2: Create runtime infrastructure
    let (router, ingress, ingress_config, _scheduler, runtime) = runtime::init(&config, &store)?;
    tracing::info!("Runtime initialized");

    // Mark storage ready
    runtime.mark_storage_ready();

    // Step 3: Register domain actors
    domains::setup(&router, &store, &runtime.admin_read_model())?;
    tracing::info!("Domain actors registered");

    // Mark domains ready
    runtime.mark_domains_ready();

    // Step 4: Start transport listeners
    let tcp_ready = handlers::spawn_tcp_listener(
        &config,
        ingress.clone(),
        ingress_config.clone(),
        runtime.clone(),
    )
    .await?;
    let ws_ready = handlers::spawn_http_listener(
        &config,
        ingress.clone(),
        ingress_config.clone(),
        runtime.clone(),
    )
    .await?;

    // Wait for listeners to be ready before accepting traffic
    tcp_ready.await.map_err(|e| {
        Box::new(std::io::Error::other(format!(
            "TCP listener failed to start: {}",
            e
        ))) as Box<dyn std::error::Error>
    })?;
    ws_ready.await.map_err(|e| {
        Box::new(std::io::Error::other(format!(
            "WebSocket listener failed to start: {}",
            e
        ))) as Box<dyn std::error::Error>
    })?;

    // Mark startup complete
    runtime.mark_startup_complete();

    tracing::info!("Fitz broker ready");
    tracing::info!("  TCP:  {}:{}", config.bind_addr, config.tcp_port);
    tracing::info!("  HTTP: {}:{}", config.bind_addr, config.http_port);

    // Step 5: Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down Fitz broker");

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn should_define_boot_module() {
        // Placeholder: Module structure is well-defined and
        // submodules are unit-testable in isolation
    }
}
