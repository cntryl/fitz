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
mod shutdown;
pub mod stats;
pub mod storage;

pub use resource_limits::enforce_startup_resource_limits;
pub use runtime::{BootConfig, BootResult};
use shutdown::{ShutdownCoordinator, ShutdownSignal};
pub use stats::{BrokerLifecycleState, Runtime};
use std::sync::Arc;
use std::time::Duration;

struct ShutdownContext {
    runtime: Runtime,
    ingress: Arc<crate::api::runtime_ingress::RuntimeIngress>,
    router: Arc<crate::runtime::Router>,
    store: Arc<cntryl_midge::Engine>,
    ws_shutdown: tokio::sync::oneshot::Sender<()>,
    ws_join: tokio::task::JoinHandle<()>,
    metrics_shutdown: tokio::sync::oneshot::Sender<()>,
    metrics_join: tokio::task::JoinHandle<()>,
    tcp_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    tcp_join: Option<tokio::task::JoinHandle<()>>,
}

type ShutdownCore = (
    Runtime,
    Arc<crate::api::runtime_ingress::RuntimeIngress>,
    Arc<crate::runtime::Router>,
    Arc<cntryl_midge::Engine>,
);
type ListenerShutdown = (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
);

fn build_shutdown_context(core: ShutdownCore, listeners: ListenerShutdown) -> ShutdownContext {
    let (runtime, ingress, router, store) = core;
    let (ws_shutdown, ws_join, metrics_shutdown, metrics_join) = listeners;
    ShutdownContext {
        runtime,
        ingress,
        router,
        store,
        ws_shutdown,
        ws_join,
        metrics_shutdown,
        metrics_join,
        tcp_shutdown: None,
        tcp_join: None,
    }
}

#[derive(Default)]
struct BootListenerBindings {
    http: Option<tokio::net::TcpListener>,
    metrics: Option<tokio::net::TcpListener>,
    tcp: Option<tokio::net::TcpListener>,
}

impl BootListenerBindings {
    fn configured() -> Self {
        Self::default()
    }

    #[cfg(test)]
    fn prebound(
        http: tokio::net::TcpListener,
        metrics: tokio::net::TcpListener,
        tcp: Option<tokio::net::TcpListener>,
    ) -> Self {
        Self {
            http: Some(http),
            metrics: Some(metrics),
            tcp,
        }
    }
}

enum BootStage<T> {
    Continue(T),
    ShutdownRequested,
    Failed(Box<dyn std::error::Error>),
}

fn failed_stage<T>(result: BootResult<()>) -> BootStage<T> {
    match result {
        Err(error) => BootStage::Failed(error),
        Ok(()) => unreachable!("failure cleanup must preserve the startup error"),
    }
}

struct StartedListeners {
    ws_shutdown: tokio::sync::oneshot::Sender<()>,
    ws_join: tokio::task::JoinHandle<()>,
    metrics_shutdown: tokio::sync::oneshot::Sender<()>,
    metrics_join: tokio::task::JoinHandle<()>,
}

async fn start_listeners(
    bindings: &mut BootListenerBindings,
    config: &BootConfig,
    ingress: Arc<crate::api::runtime_ingress::RuntimeIngress>,
    ingress_config: crate::api::ingress::IngressConfig,
    runtime: Runtime,
) -> BootStage<StartedListeners> {
    let ws_listener =
        match start_http_listener(bindings, config, ingress, ingress_config, runtime.clone()).await
        {
            Ok(listener) => listener,
            Err(error) => return BootStage::Failed(error),
        };
    let crate::api::handlers::ListenerHandle {
        ready: ws_ready,
        shutdown: ws_shutdown,
        join: ws_join,
    } = ws_listener;
    if let Err(error) = ws_ready.await {
        return failed_stage(
            abort_startup(
                &runtime,
                ws_shutdown,
                ws_join,
                None,
                None,
                Box::new(std::io::Error::other(format!(
                    "WebSocket listener failed to start: {error}"
                ))),
            )
            .await,
        );
    }

    let metrics_listener = match start_metrics_listener(bindings, config, runtime.clone()).await {
        Ok(listener) => listener,
        Err(error) => {
            return failed_stage(
                abort_startup(&runtime, ws_shutdown, ws_join, None, None, error).await,
            );
        }
    };
    let crate::api::handlers::ListenerHandle {
        ready: metrics_ready,
        shutdown: metrics_shutdown,
        join: metrics_join,
    } = metrics_listener;
    if let Err(error) = metrics_ready.await {
        return failed_stage(
            abort_startup(
                &runtime,
                ws_shutdown,
                ws_join,
                Some(metrics_shutdown),
                Some(metrics_join),
                Box::new(std::io::Error::other(format!(
                    "Metrics listener failed to start: {error}"
                ))),
            )
            .await,
        );
    }

    BootStage::Continue(StartedListeners {
        ws_shutdown,
        ws_join,
        metrics_shutdown,
        metrics_join,
    })
}

async fn open_storage_stage(
    config: &BootConfig,
    shutdown: tokio::sync::watch::Receiver<Option<ShutdownSignal>>,
) -> BootStage<Arc<cntryl_midge::Engine>> {
    match storage::init_with_shutdown(config, shutdown).await {
        Ok(storage::StorageInitOutcome::Ready(store)) => BootStage::Continue(store),
        Ok(storage::StorageInitOutcome::ShutdownRequested) => BootStage::ShutdownRequested,
        Err(error) => BootStage::Failed(error),
    }
}

fn register_domains_stage(
    router: &Arc<crate::runtime::Router>,
    store: &Arc<cntryl_midge::Engine>,
    runtime: &Runtime,
    config: &BootConfig,
) -> BootStage<Arc<domains::DomainHandles>> {
    let options = domains::DomainSetupOptions {
        route_families: config.route_families.clone(),
        schedule_write_options: config.schedule_write_options(),
        queue_write_options: config.queue_write_options(),
        queue_fast_flush_interval: config.queue_fast_flush_interval(),
        request_sync_write_options: config.request_sync_write_options(),
        request_buffered_write_options: config.request_buffered_write_options(),
        rpc_request_timeout: None,
        stream_storage_layout: config.stream_storage_layout,
        kv_idle_transaction_ttl: Duration::from_secs(config.kv_idle_transaction_ttl_seconds),
        schedule_preload_timeout: config.schedule_preload_timeout(),
    };
    match domains::setup(router, store, &runtime.admin_read_model(), &options) {
        Ok(handles) => BootStage::Continue(handles),
        Err(error) => BootStage::Failed(error),
    }
}

/// Complete broker boot sequence
///
/// # Steps
/// 0. Initialize observability (tracing, metrics, OTEL)
/// 1. Create runtime (router, ingress, family actor configuration)
/// 2. Start HTTP for standby orchestration health; WebSocket upgrades remain gated
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
    let shutdown = ShutdownCoordinator::start()?;
    boot_with_shutdown(config, shutdown).await
}

async fn boot_with_shutdown(config: BootConfig, shutdown: ShutdownCoordinator) -> BootResult<()> {
    boot_with_shutdown_and_listeners(config, shutdown, BootListenerBindings::configured()).await
}

async fn boot_with_shutdown_and_listeners(
    config: BootConfig,
    mut shutdown: ShutdownCoordinator,
    mut listener_bindings: BootListenerBindings,
) -> BootResult<()> {
    resource_limits::enforce_startup_resource_limits()?;

    let _metrics = observability::init_observability()?;

    tracing::info!("Starting Fitz broker");
    config.validate()?;
    runtime::warn_defaulted_fast_queue_policy(&config);
    if let Some(signal) = shutdown.requested() {
        tracing::info!(
            signal = signal.as_str(),
            "Shutdown requested before Fitz startup"
        );
        return Ok(());
    }

    let (router, ingress, ingress_config, runtime) =
        initialize_runtime_stage(&config, &mut shutdown)?;

    let listeners = match start_listeners(
        &mut listener_bindings,
        &config,
        ingress.clone(),
        ingress_config.clone(),
        runtime.clone(),
    )
    .await
    {
        BootStage::Continue(listeners) => listeners,
        BootStage::Failed(error) => return Err(error),
        BootStage::ShutdownRequested => return Ok(()),
    };
    let StartedListeners {
        ws_shutdown,
        ws_join,
        metrics_shutdown,
        metrics_join,
    } = listeners;

    let store = match open_storage_stage(&config, shutdown.receiver()).await {
        BootStage::Continue(store) => store,
        BootStage::ShutdownRequested => {
            let signal = shutdown
                .requested()
                .ok_or("storage startup stopped without a shutdown request")?;
            return shutdown_incomplete_startup(
                signal,
                &runtime,
                ws_shutdown,
                ws_join,
                metrics_shutdown,
                metrics_join,
            )
            .await;
        }
        BootStage::Failed(error) => {
            return abort_startup(
                &runtime,
                ws_shutdown,
                ws_join,
                Some(metrics_shutdown),
                Some(metrics_join),
                error,
            )
            .await;
        }
    };
    tracing::info!("Storage initialized");
    let shutdown_context = build_shutdown_context(
        (runtime.clone(), ingress.clone(), router.clone(), store),
        (ws_shutdown, ws_join, metrics_shutdown, metrics_join),
    );

    if let Some(signal) = shutdown.requested() {
        return shutdown_broker(signal, shutdown.receiver(), shutdown_context).await;
    }

    runtime.mark_storage_ready();
    shutdown.monitor_storage(runtime.clone(), Arc::downgrade(&shutdown_context.store));

    let domains = match register_domains_stage(&router, &shutdown_context.store, &runtime, &config)
    {
        BootStage::Continue(domains) => domains,
        BootStage::Failed(error) => {
            return fail_started_broker(error, shutdown_context).await;
        }
        BootStage::ShutdownRequested => {
            return Err("domain registration ended without a result".into());
        }
    };
    runtime.attach_domains(domains);
    tracing::info!("Domain actors registered");

    complete_startup(
        config,
        shutdown,
        listener_bindings,
        ingress,
        ingress_config,
        runtime,
        shutdown_context,
    )
    .await
}

fn initialize_runtime_stage(
    config: &BootConfig,
    shutdown: &mut ShutdownCoordinator,
) -> BootResult<(
    Arc<crate::runtime::Router>,
    Arc<crate::api::runtime_ingress::RuntimeIngress>,
    crate::api::ingress::IngressConfig,
    Runtime,
)> {
    // Runtime exists before storage so orchestration can observe a standby
    // waiting for the single-writer lease while customer traffic stays gated.
    let (router, ingress, ingress_config, runtime) = runtime::init(config)?;
    shutdown.monitor_runtime(runtime.clone());
    runtime.mark_auth_config_ready();
    tracing::info!("Runtime initialized");
    Ok((router, ingress, ingress_config, runtime))
}

async fn complete_startup(
    config: BootConfig,
    mut shutdown: ShutdownCoordinator,
    mut listener_bindings: BootListenerBindings,
    ingress: Arc<crate::api::runtime_ingress::RuntimeIngress>,
    ingress_config: crate::api::ingress::IngressConfig,
    runtime: Runtime,
    mut shutdown_context: ShutdownContext,
) -> BootResult<()> {
    // Mark domains ready
    runtime.mark_domains_ready();

    if let Some(signal) = shutdown.requested() {
        return shutdown_broker(signal, shutdown.receiver(), shutdown_context).await;
    }

    // Step 4: Start TCP listener only after domain actors exist. WebSocket
    // has been listening since standby health came online, but its upgrade path
    // stays closed until startup is complete.
    let tcp_listener = start_tcp_listener(
        &mut listener_bindings,
        &config,
        ingress.clone(),
        ingress_config.clone(),
        runtime.clone(),
    )
    .await;
    let tcp_listener = match tcp_listener {
        Ok(listener) => listener,
        Err(error) => {
            return fail_started_broker(error, shutdown_context).await;
        }
    };

    let (tcp_shutdown, tcp_join) = if let Some(tcp_listener) = tcp_listener {
        let crate::api::handlers::ListenerHandle {
            ready: tcp_ready,
            shutdown,
            join,
        } = tcp_listener;
        if let Err(error) = tcp_ready.await {
            shutdown_context.tcp_shutdown = Some(shutdown);
            shutdown_context.tcp_join = Some(join);
            return fail_started_broker(
                Box::new(std::io::Error::other(format!(
                    "TCP listener failed to start: {error}"
                ))) as Box<dyn std::error::Error>,
                shutdown_context,
            )
            .await;
        }
        (Some(shutdown), Some(join))
    } else {
        (None, None)
    };
    shutdown_context.tcp_shutdown = tcp_shutdown;
    shutdown_context.tcp_join = tcp_join;

    if let Some(signal) = shutdown.requested() {
        return shutdown_broker(signal, shutdown.receiver(), shutdown_context).await;
    }

    // Mark startup complete
    runtime.mark_startup_complete();

    log_ready_endpoints(&config, runtime.family_actor_shard_count());

    // Step 5: Wait for shutdown signal
    let shutdown_signal = shutdown.wait().await?;
    shutdown_broker(shutdown_signal, shutdown.receiver(), shutdown_context).await
}

async fn wait_for_listener(
    name: &'static str,
    mut join: tokio::task::JoinHandle<()>,
) -> Result<(), String> {
    if let Ok(result) = tokio::time::timeout(std::time::Duration::from_secs(6), &mut join).await {
        result.map_err(|error| format!("{name} listener join failed: {error}"))
    } else {
        join.abort();
        let _ = join.await;
        Err(format!(
            "{name} listener shutdown timed out and was aborted"
        ))
    }
}

fn log_ready_endpoints(config: &BootConfig, family_actor_shards: usize) {
    tracing::info!("Fitz broker ready");
    if config.tcp_enabled {
        tracing::info!("  TCP:  {}:{}", config.bind_addr, config.tcp_port);
    } else {
        tracing::info!("  TCP:  disabled");
    }
    tracing::info!("  HTTP: {}:{}", config.bind_addr, config.http_port);
    tracing::info!(
        "  Metrics: {}:{} (unauthenticated Prometheus)",
        config.metrics_bind_addr,
        config.metrics_port
    );
    tracing::info!(family_actor_shards, "  Family actor shards");
}

async fn start_http_listener(
    listener_bindings: &mut BootListenerBindings,
    config: &BootConfig,
    ingress: Arc<crate::api::runtime_ingress::RuntimeIngress>,
    ingress_config: crate::api::ingress::IngressConfig,
    runtime: Runtime,
) -> BootResult<crate::api::handlers::ListenerHandle> {
    if let Some(listener) = listener_bindings.http.take() {
        return crate::api::handlers::spawn_http_listener_with_bound_socket(
            listener,
            ingress,
            &ingress_config,
            runtime,
            config.ws_allowed_origins.clone(),
        );
    }

    crate::api::handlers::spawn_http_listener(config, ingress, ingress_config, runtime).await
}

async fn start_metrics_listener(
    listener_bindings: &mut BootListenerBindings,
    config: &BootConfig,
    runtime: Runtime,
) -> BootResult<crate::api::handlers::ListenerHandle> {
    if let Some(listener) = listener_bindings.metrics.take() {
        return crate::api::handlers::spawn_metrics_listener_with_bound_socket(listener, runtime);
    }

    crate::api::handlers::spawn_metrics_listener(config, runtime).await
}

async fn abort_startup<T>(
    runtime: &Runtime,
    ws_shutdown: tokio::sync::oneshot::Sender<()>,
    ws_join: tokio::task::JoinHandle<()>,
    metrics_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    metrics_join: Option<tokio::task::JoinHandle<()>>,
    error: Box<dyn std::error::Error>,
) -> BootResult<T> {
    runtime.begin_shutdown();
    let _ = ws_shutdown.send(());
    if let Some(metrics_shutdown) = metrics_shutdown {
        let _ = metrics_shutdown.send(());
    }
    let metrics_wait = async {
        match metrics_join {
            Some(join) => wait_for_listener("Metrics", join).await,
            None => Ok(()),
        }
    };
    let (http_result, metrics_result) =
        tokio::join!(wait_for_listener("HTTP", ws_join), metrics_wait);
    match combine_cleanup_results([http_result, metrics_result]) {
        Ok(()) => Err(error),
        Err(cleanup_error) => {
            Err(format!("{error}; startup listener cleanup failed: {cleanup_error}").into())
        }
    }
}

async fn shutdown_incomplete_startup(
    signal: ShutdownSignal,
    runtime: &Runtime,
    ws_shutdown: tokio::sync::oneshot::Sender<()>,
    ws_join: tokio::task::JoinHandle<()>,
    metrics_shutdown: tokio::sync::oneshot::Sender<()>,
    metrics_join: tokio::task::JoinHandle<()>,
) -> BootResult<()> {
    tracing::info!(
        signal = signal.as_str(),
        "Stopping Fitz before storage startup completed"
    );
    runtime.begin_shutdown();
    let _ = ws_shutdown.send(());
    let _ = metrics_shutdown.send(());
    let (http_result, metrics_result) = tokio::join!(
        wait_for_listener("HTTP", ws_join),
        wait_for_listener("Metrics", metrics_join)
    );
    combine_cleanup_results([http_result, metrics_result]).map_err(Into::into)
}

fn combine_cleanup_results<const N: usize>(results: [Result<(), String>; N]) -> Result<(), String> {
    let errors = results
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

async fn start_tcp_listener(
    listener_bindings: &mut BootListenerBindings,
    config: &BootConfig,
    ingress: Arc<crate::api::runtime_ingress::RuntimeIngress>,
    ingress_config: crate::api::ingress::IngressConfig,
    runtime: Runtime,
) -> BootResult<Option<crate::api::handlers::ListenerHandle>> {
    if !config.tcp_enabled {
        tracing::info!("TCP listener disabled");
        return Ok(None);
    }

    if let Some(listener) = listener_bindings.tcp.take() {
        return crate::api::handlers::spawn_tcp_listener_with_bound_socket(
            listener,
            ingress,
            &ingress_config,
            runtime,
        )
        .map(Some);
    }

    crate::api::handlers::spawn_tcp_listener(config, ingress, ingress_config, runtime)
        .await
        .map(Some)
}

async fn shutdown_broker(
    signal: ShutdownSignal,
    mut shutdown_events: tokio::sync::watch::Receiver<Option<ShutdownSignal>>,
    context: ShutdownContext,
) -> BootResult<()> {
    tracing::info!(signal = signal.as_str(), "Shutting down Fitz broker");

    let (session_close_reason, signal) =
        session_close_reason(&context.runtime, signal, &mut shutdown_events).await;
    let shutdown_result = shutdown_context(context, session_close_reason).await;
    let signal = (*shutdown_events.borrow()).unwrap_or(signal);
    if signal.is_failure() {
        let failure = match signal {
            ShutdownSignal::ActorFailure => "fatal domain actor failure",
            ShutdownSignal::StorageLeaseFailure => "primary Midge writer lease became unhealthy",
            ShutdownSignal::CtrlC | ShutdownSignal::Sigterm | ShutdownSignal::DrainRequested => {
                unreachable!()
            }
        };
        return match shutdown_result {
            Ok(()) => Err(failure.into()),
            Err(cleanup_error) => {
                Err(format!("{failure}; broker cleanup failed: {cleanup_error}").into())
            }
        };
    }
    shutdown_result
}

async fn fail_started_broker<T>(
    error: Box<dyn std::error::Error>,
    context: ShutdownContext,
) -> BootResult<T> {
    let cleanup_result = shutdown_context(context, "broker startup failed".to_string()).await;
    match cleanup_result {
        Ok(()) => Err(error),
        Err(cleanup_error) => {
            Err(format!("{error}; broker startup cleanup failed: {cleanup_error}").into())
        }
    }
}

async fn shutdown_context(context: ShutdownContext, close_reason: String) -> BootResult<()> {
    context.runtime.begin_shutdown();
    context
        .ingress
        .close_all_sessions(crate::session::CloseReason::ServerClose(close_reason))
        .await;
    context.ingress.drain_session_cleanups().await;

    if let Some(tcp_shutdown) = context.tcp_shutdown {
        let _ = tcp_shutdown.send(());
    }
    let _ = context.ws_shutdown.send(());
    let _ = context.metrics_shutdown.send(());

    let tcp_wait = async {
        match context.tcp_join {
            Some(join) => wait_for_listener("TCP", join).await,
            None => Ok(()),
        }
    };
    let (tcp_result, http_result, metrics_result) = tokio::join!(
        tcp_wait,
        wait_for_listener("HTTP", context.ws_join),
        wait_for_listener("Metrics", context.metrics_join)
    );
    let listener_result = combine_cleanup_results([tcp_result, http_result, metrics_result]);
    let runtime_result = shutdown_runtime(
        context.runtime,
        context.ingress,
        context.router,
        context.store,
    )
    .await;

    match (listener_result, runtime_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(listener_error), Ok(())) => Err(listener_error.into()),
        (Ok(()), Err(runtime_error)) => Err(runtime_error),
        (Err(listener_error), Err(runtime_error)) => Err(format!(
            "listener cleanup failed: {listener_error}; runtime cleanup failed: {runtime_error}"
        )
        .into()),
    }
}

async fn session_close_reason(
    runtime: &Runtime,
    signal: ShutdownSignal,
    shutdown_events: &mut tokio::sync::watch::Receiver<Option<ShutdownSignal>>,
) -> (String, ShutdownSignal) {
    if signal.requires_drain() {
        runtime.begin_drain();
        if runtime.is_startup_complete() {
            let remaining = runtime.remaining_drain_grace();
            tracing::info!(
                drain_grace_ms = remaining.as_millis(),
                "Fitz broker draining before shutdown"
            );
            if !remaining.is_zero() {
                let drain = tokio::time::sleep(remaining);
                tokio::pin!(drain);
                loop {
                    if let Some(escalated) = *shutdown_events.borrow() {
                        if !escalated.requires_drain() {
                            tracing::warn!(
                                initial_signal = signal.as_str(),
                                escalated_signal = escalated.as_str(),
                                "Immediate shutdown preempted the planned drain grace"
                            );
                            return (immediate_close_reason(escalated), escalated);
                        }
                    }
                    tokio::select! {
                        biased;
                        changed = shutdown_events.changed() => {
                            if changed.is_err() {
                                break;
                            }
                        }
                        () = &mut drain => break,
                    }
                }
            }
        }
        let effective_signal = shutdown_events.borrow().unwrap_or(signal);
        if !effective_signal.requires_drain() {
            return (immediate_close_reason(effective_signal), effective_signal);
        }
        return (runtime.drain_close_reason(), effective_signal);
    }

    (immediate_close_reason(signal), signal)
}

fn immediate_close_reason(signal: ShutdownSignal) -> String {
    match signal {
        ShutdownSignal::CtrlC => "broker shutdown".to_string(),
        ShutdownSignal::ActorFailure => {
            "broker stopped after fatal domain actor failure".to_string()
        }
        ShutdownSignal::StorageLeaseFailure => {
            "broker stopped after losing the Midge writer lease".to_string()
        }
        ShutdownSignal::Sigterm | ShutdownSignal::DrainRequested => unreachable!(),
    }
}

async fn shutdown_runtime(
    runtime: Runtime,
    ingress: Arc<crate::api::runtime_ingress::RuntimeIngress>,
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

    storage::shutdown_store(store).await
}

#[cfg(test)]
mod tests;
