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
use std::sync::Arc;

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

struct ShutdownContext {
    runtime: Runtime,
    ingress: Arc<crate::session::manager::RuntimeIngress>,
    router: Arc<crate::runtime::Router>,
    store: Arc<cntryl_midge::Engine>,
    ws_shutdown: tokio::sync::oneshot::Sender<()>,
    ws_join: tokio::task::JoinHandle<()>,
    tcp_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    tcp_join: Option<tokio::task::JoinHandle<()>>,
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
///
/// # Errors
///
/// Returns an error when observability, runtime, storage, domain, transport,
/// or shutdown coordination fails during broker boot or teardown.
pub async fn boot(config: BootConfig) -> BootResult<()> {
    resource_limits::enforce_startup_resource_limits()?;

    // Step 0: Initialize observability (tracing, metrics, OTEL)
    let _metrics = observability::init_observability()?;

    tracing::info!("Starting Fitz broker");
    config.validate()?;
    warn_defaulted_fast_queue_policy(&config);

    // Step 1: Create runtime infrastructure before opening storage so HTTP
    // target health can participate in ECS handoff while Midge waits for the
    // single-writer lease.
    let (router, ingress, ingress_config, runtime) = runtime::init(&config)?;
    tracing::info!("Runtime initialized");

    runtime.mark_auth_config_ready();

    let ws_listener = start_http_listener(
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
            "WebSocket listener failed to start: {e}"
        ))) as Box<dyn std::error::Error>
    })?;

    // Step 2: Open storage after HTTP target health is reachable.
    let store = match storage::init(&config).await {
        Ok(store) => store,
        Err(error) => return abort_startup(ws_shutdown, ws_join, error).await,
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
        &domains::DomainSetupOptions {
            server_write_options,
            queue_write_options,
            queue_fast_flush_interval: config.queue_fast_flush_interval(),
            request_sync_write_options: config.request_sync_write_options(),
            rpc_request_timeout: None,
            stream_storage_layout: config.stream_storage_layout,
        },
    ) {
        Ok(domains) => domains,
        Err(error) => return abort_startup(ws_shutdown, ws_join, error).await,
    };
    runtime.attach_domains(domains);
    tracing::info!("Domain actors registered");

    // Mark domains ready
    runtime.mark_domains_ready();

    // Step 4: Start TCP listener only after domain actors exist. WebSocket
    // has been listening since target health came online, but its upgrade path
    // stays closed until startup is complete.
    let tcp_listener = start_tcp_listener(
        &config,
        ingress.clone(),
        ingress_config.clone(),
        runtime.clone(),
    )
    .await;
    let tcp_listener = match tcp_listener {
        Ok(listener) => listener,
        Err(error) => return abort_startup(ws_shutdown, ws_join, error).await,
    };

    let (tcp_shutdown, tcp_join) = if let Some(tcp_listener) = tcp_listener {
        let crate::api::handlers::ListenerHandle {
            ready: tcp_ready,
            shutdown,
            join,
        } = tcp_listener;
        tcp_ready.await.map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "TCP listener failed to start: {e}"
            ))) as Box<dyn std::error::Error>
        })?;
        (Some(shutdown), Some(join))
    } else {
        (None, None)
    };

    // Mark startup complete
    runtime.mark_startup_complete();

    log_ready_endpoints(&config);

    // Step 5: Wait for shutdown signal
    shutdown_broker(
        wait_for_shutdown_signal().await?,
        ShutdownContext {
            runtime,
            ingress,
            router,
            store,
            ws_shutdown,
            ws_join,
            tcp_shutdown,
            tcp_join,
        },
    )
    .await
}

async fn wait_for_listener(
    name: &'static str,
    join: tokio::task::JoinHandle<()>,
) -> Result<(), String> {
    tokio::time::timeout(std::time::Duration::from_secs(6), join)
        .await
        .map_err(|_| format!("{name} listener shutdown timed out"))?
        .map_err(|error| format!("{name} listener join failed: {error}"))
}

fn log_ready_endpoints(config: &BootConfig) {
    tracing::info!("Fitz broker ready");
    if config.tcp_enabled {
        tracing::info!("  TCP:  {}:{}", config.bind_addr, config.tcp_port);
    } else {
        tracing::info!("  TCP:  disabled");
    }
    tracing::info!("  HTTP: {}:{}", config.bind_addr, config.http_port);
}

fn warn_defaulted_fast_queue_policy(config: &BootConfig) {
    if !config.queue_write_policy_defaulted_fast() {
        return;
    }

    tracing::warn!(
        queue_write_policy_env = "FITZ_QUEUE_WRITE_POLICY",
        queue_loss_window_env = "FITZ_QUEUE_LOSS_WINDOW_MS",
        loss_window_ms = config.queue_loss_window_ms,
        loss_window = ?config.queue_fast_flush_interval(),
        "FITZ_QUEUE_WRITE_POLICY is unset; defaulting Queue to fast best-effort writes"
    );
}

async fn start_http_listener(
    config: &BootConfig,
    ingress: Arc<crate::session::manager::RuntimeIngress>,
    ingress_config: crate::api::ingress::IngressConfig,
    runtime: Runtime,
) -> BootResult<crate::api::handlers::ListenerHandle> {
    crate::api::handlers::spawn_http_listener(config, ingress, ingress_config, runtime).await
}

async fn abort_startup<T>(
    ws_shutdown: tokio::sync::oneshot::Sender<()>,
    ws_join: tokio::task::JoinHandle<()>,
    error: Box<dyn std::error::Error>,
) -> BootResult<T> {
    let _ = ws_shutdown.send(());
    wait_for_listener("HTTP", ws_join).await?;
    Err(error)
}

async fn start_tcp_listener(
    config: &BootConfig,
    ingress: Arc<crate::session::manager::RuntimeIngress>,
    ingress_config: crate::api::ingress::IngressConfig,
    runtime: Runtime,
) -> BootResult<Option<crate::api::handlers::ListenerHandle>> {
    if !config.tcp_enabled {
        tracing::info!("TCP listener disabled");
        return Ok(None);
    }

    crate::api::handlers::spawn_tcp_listener(config, ingress, ingress_config, runtime)
        .await
        .map(Some)
}

async fn shutdown_broker(signal: ShutdownSignal, context: ShutdownContext) -> BootResult<()> {
    tracing::info!(signal = signal.as_str(), "Shutting down Fitz broker");

    let session_close_reason = session_close_reason(&context.runtime, &signal).await;
    context.runtime.begin_shutdown();
    context
        .ingress
        .close_all_sessions(crate::session::CloseReason::ServerClose(
            session_close_reason,
        ))
        .await;

    if let Some(tcp_shutdown) = context.tcp_shutdown {
        let _ = tcp_shutdown.send(());
    }
    let _ = context.ws_shutdown.send(());

    if let Some(tcp_join) = context.tcp_join {
        wait_for_listener("TCP", tcp_join).await?;
    }
    wait_for_listener("HTTP", context.ws_join).await?;

    shutdown_runtime(
        context.runtime,
        context.ingress,
        context.router,
        context.store,
    )
}

async fn session_close_reason(runtime: &Runtime, signal: &ShutdownSignal) -> String {
    match signal {
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
    }
}

fn shutdown_runtime(
    runtime: Runtime,
    ingress: Arc<crate::session::manager::RuntimeIngress>,
    router: Arc<crate::runtime::Router>,
    store: Arc<cntryl_midge::Engine>,
) -> BootResult<()> {
    let domains = runtime.detach_domains();
    if let Some(domains) = &domains {
        domains.stop();
    }
    router.clear();
    let _ = runtime.detach_ingress();
    drop(domains);
    drop(ingress);
    drop(router);
    drop(runtime);

    let store = Arc::try_unwrap(store).map_err(|store| {
        format!(
            "Midge shutdown blocked by {} leftover engine references",
            Arc::strong_count(&store)
        )
    })?;
    store
        .shutdown()
        .map_err(|error| format!("Midge shutdown failed: {error}"))?;

    Ok(())
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
