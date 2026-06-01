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
    config.validate()?;

    // Step 1: Open storage
    let store = storage::init(&config).await?;
    tracing::info!("Storage initialized");

    // Step 2: Create runtime infrastructure
    let (router, ingress, ingress_config, runtime) = runtime::init(&config, &store)?;
    tracing::info!("Runtime initialized");

    // Mark storage ready
    runtime.mark_storage_ready();

    // Step 3: Register domain actors
    let queue_write_options = if matches!(&config.storage_mode, runtime::StorageMode::Memory) {
        cntryl_midge::WriteOptions::best_effort()
    } else {
        cntryl_midge::WriteOptions::buffered()
    };
    let domains = domains::setup(
        &router,
        &store,
        &runtime.admin_read_model(),
        queue_write_options,
        None,
        config.stream_storage_layout,
    )?;
    runtime.attach_domains(std::sync::Arc::new(domains));
    tracing::info!("Domain actors registered");

    // Mark domains ready
    runtime.mark_domains_ready();

    // Step 4: Start transport listeners
    let tcp_listener = crate::api::handlers::spawn_tcp_listener(
        &config,
        ingress.clone(),
        ingress_config.clone(),
        runtime.clone(),
    )
    .await?;
    let ws_listener = crate::api::handlers::spawn_http_listener(
        &config,
        ingress.clone(),
        ingress_config.clone(),
        runtime.clone(),
    )
    .await?;

    let crate::api::handlers::ListenerHandle {
        ready: tcp_ready,
        shutdown: tcp_shutdown,
        join: tcp_join,
    } = tcp_listener;
    let crate::api::handlers::ListenerHandle {
        ready: ws_ready,
        shutdown: ws_shutdown,
        join: ws_join,
    } = ws_listener;

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

    let _ = tcp_shutdown.send(());
    let _ = ws_shutdown.send(());

    let wait_for_listener = async |name, join: tokio::task::JoinHandle<()>| {
        tokio::time::timeout(std::time::Duration::from_secs(6), join)
            .await
            .map_err(|_| format!("{} listener shutdown timed out", name))?
            .map_err(|error| format!("{} listener join failed: {}", name, error))
    };
    let (tcp_result, ws_result) = tokio::join!(
        wait_for_listener("TCP", tcp_join),
        wait_for_listener("HTTP", ws_join)
    );
    tcp_result?;
    ws_result?;

    let domains = runtime.detach_domains();
    if let Some(domains) = &domains {
        domains.stop();
    }
    ingress
        .close_all_sessions(crate::session::CloseReason::ServerClose(
            "broker shutdown".to_string(),
        ))
        .await;
    router.clear();
    runtime.detach_ingress();
    drop(domains);
    drop(ingress);
    drop(router);
    drop(runtime);

    let store = std::sync::Arc::try_unwrap(store).map_err(|store| {
        format!(
            "Midge shutdown blocked by {} leftover engine references",
            std::sync::Arc::strong_count(&store)
        )
    })?;
    store
        .shutdown()
        .map_err(|error| format!("Midge shutdown failed: {}", error))?;

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
