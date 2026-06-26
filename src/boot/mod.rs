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
pub mod resource_limits;
pub mod runtime;
pub mod stats;
pub mod storage;

pub use resource_limits::enforce_startup_resource_limits;
pub use runtime::{BootConfig, BootResult};
pub use stats::{BrokerLifecycleState, Runtime};

enum ShutdownSignal {
    CtrlC,
    Sigterm,
}

impl ShutdownSignal {
    fn as_str(&self) -> &'static str {
        match self {
            Self::CtrlC => "ctrl_c",
            Self::Sigterm => "sigterm",
        }
    }
}

/// Complete broker boot sequence
///
/// # Steps
/// 0. Initialize observability (tracing, metrics, OTEL)
/// 1. Create runtime (router, ingress, scheduler)
/// 2. Start HTTP for target health; WebSocket upgrades remain gated
/// 3. Open storage and acquire the active Midge writer lease
/// 4. Register domain actors
/// 5. Start TCP listener
/// 6. Wait for shutdown signal
/// 7. Graceful shutdown
pub async fn boot(config: BootConfig) -> BootResult<()> {
    resource_limits::enforce_startup_resource_limits()?;

    // Step 0: Initialize observability (tracing, metrics, OTEL)
    let _metrics = observability::init_observability()?;

    tracing::info!("Starting Fitz broker");
    config.validate()?;

    // Step 1: Create runtime infrastructure before opening storage so HTTP
    // target health can participate in ECS handoff while Midge waits for the
    // single-writer lease.
    let (router, ingress, ingress_config, runtime) = runtime::init(&config)?;
    tracing::info!("Runtime initialized");

    runtime.mark_auth_config_ready();

    let ws_listener = crate::api::handlers::spawn_http_listener(
        &config,
        ingress.clone(),
        ingress_config.clone(),
        runtime.clone(),
    )
    .await?;
    let crate::api::handlers::ListenerHandle {
        ready: ws_ready,
        shutdown: ws_shutdown,
        join: ws_join,
    } = ws_listener;

    ws_ready.await.map_err(|e| {
        Box::new(std::io::Error::other(format!(
            "WebSocket listener failed to start: {}",
            e
        ))) as Box<dyn std::error::Error>
    })?;

    // Step 2: Open storage after HTTP target health is reachable.
    let store = match storage::init(&config).await {
        Ok(store) => store,
        Err(error) => {
            let _ = ws_shutdown.send(());
            wait_for_listener("HTTP", ws_join).await?;
            return Err(error);
        }
    };
    tracing::info!("Storage initialized");

    runtime.mark_storage_ready();

    // Step 3: Register domain actors
    let server_write_options = config.server_write_options();
    let queue_write_options = config.queue_write_options();
    let domains = match domains::setup(
        &router,
        &store,
        &runtime.admin_read_model(),
        domains::DomainSetupOptions {
            server_write_options,
            queue_write_options,
            queue_fast_flush_interval: config.queue_fast_flush_interval(),
            request_sync_write_options: config.request_sync_write_options(),
            rpc_request_timeout: None,
            stream_storage_layout: config.stream_storage_layout,
        },
    ) {
        Ok(domains) => domains,
        Err(error) => {
            let _ = ws_shutdown.send(());
            wait_for_listener("HTTP", ws_join).await?;
            return Err(error);
        }
    };
    runtime.attach_domains(std::sync::Arc::new(domains));
    tracing::info!("Domain actors registered");

    // Mark domains ready
    runtime.mark_domains_ready();

    // Step 4: Start TCP listener only after domain actors exist. WebSocket
    // has been listening since target health came online, but its upgrade path
    // stays closed until startup is complete.
    let tcp_listener = if config.tcp_enabled {
        match crate::api::handlers::spawn_tcp_listener(
            &config,
            ingress.clone(),
            ingress_config.clone(),
            runtime.clone(),
        )
        .await
        {
            Ok(listener) => Some(listener),
            Err(error) => {
                let _ = ws_shutdown.send(());
                wait_for_listener("HTTP", ws_join).await?;
                return Err(error);
            }
        }
    } else {
        tracing::info!("TCP listener disabled");
        None
    };

    let (tcp_shutdown, tcp_join) = if let Some(tcp_listener) = tcp_listener {
        let crate::api::handlers::ListenerHandle {
            ready: tcp_ready,
            shutdown,
            join,
        } = tcp_listener;
        tcp_ready.await.map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "TCP listener failed to start: {}",
                e
            ))) as Box<dyn std::error::Error>
        })?;
        (Some(shutdown), Some(join))
    } else {
        (None, None)
    };

    // Mark startup complete
    runtime.mark_startup_complete();

    tracing::info!("Fitz broker ready");
    if config.tcp_enabled {
        tracing::info!("  TCP:  {}:{}", config.bind_addr, config.tcp_port);
    } else {
        tracing::info!("  TCP:  disabled");
    }
    tracing::info!("  HTTP: {}:{}", config.bind_addr, config.http_port);

    // Step 5: Wait for shutdown signal
    let signal = wait_for_shutdown_signal().await?;
    tracing::info!(signal = signal.as_str(), "Shutting down Fitz broker");

    let session_close_reason = match signal {
        ShutdownSignal::Sigterm => {
            runtime.begin_drain();
            let remaining = runtime.remaining_drain_grace();
            tracing::info!(
                drain_grace_ms = remaining.as_millis(),
                "Fitz broker draining before shutdown"
            );
            if !remaining.is_zero() {
                tokio::time::sleep(remaining).await;
            }
            runtime.drain_close_reason()
        }
        ShutdownSignal::CtrlC => "broker shutdown".to_string(),
    };

    runtime.begin_shutdown();
    ingress
        .close_all_sessions(crate::session::CloseReason::ServerClose(
            session_close_reason,
        ))
        .await;

    if let Some(tcp_shutdown) = tcp_shutdown {
        let _ = tcp_shutdown.send(());
    }
    let _ = ws_shutdown.send(());

    if let Some(tcp_join) = tcp_join {
        wait_for_listener("TCP", tcp_join).await?;
    }
    wait_for_listener("HTTP", ws_join).await?;

    let domains = runtime.detach_domains();
    if let Some(domains) = &domains {
        domains.stop();
    }
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

async fn wait_for_listener(
    name: &'static str,
    join: tokio::task::JoinHandle<()>,
) -> Result<(), String> {
    tokio::time::timeout(std::time::Duration::from_secs(6), join)
        .await
        .map_err(|_| format!("{} listener shutdown timed out", name))?
        .map_err(|error| format!("{} listener join failed: {}", name, error))
}

async fn wait_for_shutdown_signal() -> BootResult<ShutdownSignal> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                Ok(ShutdownSignal::CtrlC)
            }
            _ = terminate.recv() => Ok(ShutdownSignal::Sigterm),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok(ShutdownSignal::CtrlC)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn should_define_boot_module() {
        // Placeholder: Module structure is well-defined and
        // submodules are unit-testable in isolation
    }
}
