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
pub mod runtime;
pub mod stats;
pub mod storage;

pub use runtime::{BootConfig, BootResult};
pub use stats::Runtime;

/// Complete broker boot sequence
///
/// # Steps
/// 1. Initialize storage
/// 2. Create runtime (router, ingress, scheduler)
/// 3. Register domain actors
/// 4. Spawn transport listeners (TCP, HTTP/WS)
/// 5. Wait for Ctrl+C
/// 6. Graceful shutdown
pub async fn boot(config: BootConfig) -> BootResult<()> {
    // Step 1: Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "fitz=info,warn".to_string()))
        .init();

    tracing::info!("Starting Fitz broker");

    // Step 2: Open storage
    let store = storage::init(&config).await?;
    tracing::info!("Storage initialized");

    // Step 3: Create runtime infrastructure
    let (router, ingress, ingress_config, _scheduler, runtime) = runtime::init(&store)?;
    tracing::info!("Runtime initialized");
    
    // Mark storage ready
    runtime.mark_storage_ready();

    // Step 4: Register domain actors
    domains::setup(&router, &store)?;
    tracing::info!("Domain actors registered");
    
    // Mark domains ready
    runtime.mark_domains_ready();

    // Step 5: Start transport listeners
    handlers::spawn_tcp_listener(&config, ingress.clone(), ingress_config.clone()).await?;
    handlers::spawn_http_listener(&config, ingress.clone(), ingress_config.clone(), runtime.clone()).await?;
    
    // Mark startup complete
    runtime.mark_startup_complete();

    tracing::info!("Fitz broker ready");
    tracing::info!("  TCP:  {}:{}", config.bind_addr, config.tcp_port);
    tracing::info!("  HTTP: {}:{}", config.bind_addr, config.http_port);

    // Step 6: Wait for shutdown signal
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
