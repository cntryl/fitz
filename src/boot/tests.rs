use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BOOT_CLEANUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);

#[test]
fn should_define_boot_module() {
    // Placeholder: Module structure is well-defined and
    // submodules are unit-testable in isolation
}

#[tokio::test]
async fn should_translate_runtime_drain_into_planned_shutdown() {
    // Arrange
    let runtime = Runtime::new(Arc::new(crate::runtime::Router::new()));
    let (mut shutdown, _trigger) = shutdown::ShutdownCoordinator::manual();
    shutdown.monitor_runtime(runtime.clone());

    // Act
    runtime.begin_drain();
    let signal = tokio::time::timeout(std::time::Duration::from_secs(1), shutdown.wait())
        .await
        .expect("runtime drain should request shutdown")
        .expect("shutdown monitor");

    // Assert
    assert_eq!(signal, shutdown::ShutdownSignal::DrainRequested);
}

#[test]
fn should_escalate_planned_shutdown_given_immediate_failure() {
    // Arrange
    let (coordinator, trigger) = shutdown::ShutdownCoordinator::manual();

    // Act
    let planned_requested = trigger.request(shutdown::ShutdownSignal::Sigterm);
    let failure_requested = trigger.request(shutdown::ShutdownSignal::ActorFailure);
    let downgrade_requested = trigger.request(shutdown::ShutdownSignal::DrainRequested);

    // Assert
    assert!(planned_requested);
    assert!(failure_requested);
    assert!(!downgrade_requested);
    assert_eq!(
        coordinator.requested(),
        Some(shutdown::ShutdownSignal::ActorFailure)
    );
}

#[test]
fn should_escalate_planned_shutdown_given_ctrl_c() {
    // Arrange
    let (coordinator, trigger) = shutdown::ShutdownCoordinator::manual();
    assert!(trigger.request(shutdown::ShutdownSignal::DrainRequested));

    // Act
    let escalated = trigger.request(shutdown::ShutdownSignal::CtrlC);

    // Assert
    assert!(escalated);
    assert_eq!(
        coordinator.requested(),
        Some(shutdown::ShutdownSignal::CtrlC)
    );
}

#[tokio::test]
async fn should_preempt_planned_drain_grace_given_fatal_actor_failure() {
    // Arrange
    let runtime = Runtime::new(Arc::new(crate::runtime::Router::new()));
    runtime.configure_drain(30, "planned replacement".to_string());
    runtime.mark_startup_complete();
    let (mut coordinator, trigger) = shutdown::ShutdownCoordinator::manual();
    coordinator.monitor_runtime(runtime.clone());
    assert!(trigger.request(shutdown::ShutdownSignal::Sigterm));
    let initial_signal = coordinator.wait().await.expect("planned shutdown signal");
    let mut shutdown_events = coordinator.receiver();
    let runtime_for_failure = runtime.clone();
    let failure = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        runtime_for_failure.mark_fatal_domain_failure();
    });

    // Act
    let (reason, effective_signal) = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        session_close_reason(&runtime, initial_signal, &mut shutdown_events),
    )
    .await
    .expect("fatal failure should preempt the drain grace");

    // Assert
    failure.await.expect("failure driver");
    assert_eq!(effective_signal, shutdown::ShutdownSignal::ActorFailure);
    assert_eq!(reason, "broker stopped after fatal domain actor failure");
}

#[test]
fn should_withdraw_readiness_plus_request_shutdown_given_storage_lease_failure() {
    // Arrange
    let runtime = Runtime::new(Arc::new(crate::runtime::Router::new()));
    runtime.mark_storage_ready();
    runtime.mark_auth_config_ready();
    runtime.mark_domains_ready();
    runtime.mark_startup_complete();
    let (coordinator, trigger) = shutdown::ShutdownCoordinator::manual();

    // Act
    shutdown::handle_storage_lease_failure(&runtime, &trigger);

    // Assert
    assert!(!runtime.is_storage_ready());
    assert!(!runtime.is_ready_for_traffic());
    assert!(runtime.is_draining());
    assert_eq!(
        coordinator.requested(),
        Some(shutdown::ShutdownSignal::StorageLeaseFailure)
    );
}

#[tokio::test]
async fn should_stop_standby_startup_given_shutdown_while_writer_lease_is_held() {
    // Arrange
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let (listener_bindings, http_port, metrics_port) = bind_standby_listeners().await;
    let config = standby_boot_config(tempdir.path(), http_port, metrics_port);
    let holder = storage::init(&config).await.expect("open active writer");
    let (coordinator, trigger) = shutdown::ShutdownCoordinator::manual();
    let driver = tokio::spawn(async move {
        wait_for_http_status(http_port, "/livez", 200).await;
        wait_for_http_status(http_port, "/targetz", 200).await;
        wait_for_http_status(http_port, "/startupz", 503).await;
        wait_for_http_status(http_port, "/healthz", 503).await;
        wait_for_http_status(http_port, "/readyz", 503).await;
        assert!(trigger.request(shutdown::ShutdownSignal::Sigterm));
    });

    // Act
    let result = tokio::time::timeout(
        BOOT_CLEANUP_TIMEOUT,
        boot_with_shutdown_and_listeners(config, coordinator, listener_bindings),
    )
    .await
    .expect("standby shutdown should complete");

    // Assert
    result.expect("standby shutdown");
    driver.await.expect("standby driver");
    assert!(
        holder.get_runtime_metrics().is_ok(),
        "holder remains active"
    );
    storage::shutdown_store(holder)
        .await
        .expect("shutdown active writer");
    assert_loopback_port_released(http_port);
    assert_loopback_port_released(metrics_port);
}

#[tokio::test]
async fn should_promote_waiting_instance_after_active_writer_shutdown() {
    // Arrange
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let (listener_bindings, http_port, metrics_port) = bind_standby_listeners().await;
    let config = standby_boot_config(tempdir.path(), http_port, metrics_port);
    let holder = storage::init(&config).await.expect("open active writer");
    let (coordinator, trigger) = shutdown::ShutdownCoordinator::manual();
    let driver = tokio::spawn(async move {
        wait_for_http_status(http_port, "/livez", 200).await;
        wait_for_http_status(http_port, "/targetz", 200).await;
        wait_for_http_status(http_port, "/startupz", 503).await;
        wait_for_http_status(http_port, "/healthz", 503).await;
        wait_for_http_status(http_port, "/readyz", 503).await;
        storage::shutdown_store(holder)
            .await
            .expect("release active writer");
        wait_for_http_status(http_port, "/healthz", 200).await;
        assert!(trigger.request(shutdown::ShutdownSignal::Sigterm));
    });

    // Act
    let result = tokio::time::timeout(
        BOOT_CLEANUP_TIMEOUT,
        boot_with_shutdown_and_listeners(config.clone(), coordinator, listener_bindings),
    )
    .await
    .expect("promoted broker shutdown should complete");

    // Assert
    result.expect("promoted broker shutdown");
    driver.await.expect("promotion driver");
    assert_loopback_port_released(http_port);
    assert_loopback_port_released(metrics_port);
    let reopened = storage::init(&config)
        .await
        .expect("reopen released storage");
    storage::shutdown_store(reopened)
        .await
        .expect("shutdown reopened storage");
}

#[tokio::test]
#[serial_test::serial]
async fn should_release_writer_lease_given_tcp_listener_startup_failure() {
    // Arrange
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let (listener_bindings, http_port, metrics_port) = bind_standby_listeners().await;
    let occupied_tcp =
        std::net::TcpListener::bind("127.0.0.1:0").expect("occupy TCP listener port");
    let tcp_port = occupied_tcp
        .local_addr()
        .expect("occupied TCP address")
        .port();
    let config = standby_boot_config(tempdir.path(), http_port, metrics_port)
        .with_tcp_enabled(true)
        .with_tcp_port(tcp_port);
    let (coordinator, _trigger) = shutdown::ShutdownCoordinator::manual();

    // Act
    let error = tokio::time::timeout(
        BOOT_CLEANUP_TIMEOUT,
        boot_with_shutdown_and_listeners(config.clone(), coordinator, listener_bindings),
    )
    .await
    .expect("failed startup cleanup should complete")
    .expect_err("occupied TCP port must fail startup");

    // Assert
    assert!(
        error.to_string().contains("Address already in use"),
        "unexpected startup error: {error}"
    );
    drop(occupied_tcp);
    assert_loopback_port_released(http_port);
    assert_loopback_port_released(metrics_port);
    let reopened = storage::init(&config).await.expect("writer lease released");
    storage::shutdown_store(reopened)
        .await
        .expect("shutdown reopened storage");
}

fn standby_boot_config(
    storage_path: &std::path::Path,
    http_port: u16,
    metrics_port: u16,
) -> BootConfig {
    BootConfig::with_local_storage(storage_path.to_string_lossy().to_string())
        .with_http_port(http_port)
        .with_tcp_enabled(false)
        .with_bind_addr("127.0.0.1".to_string())
        .with_metrics_bind_addr("127.0.0.1".to_string())
        .with_metrics_port(metrics_port)
        .with_auth_config(crate::auth::AuthConfig::Disabled)
        .with_drain_grace_seconds(1)
}

async fn bind_standby_listeners() -> (BootListenerBindings, u16, u16) {
    let http = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind HTTP listener");
    let http_port = http.local_addr().expect("HTTP listener address").port();
    let metrics = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind metrics listener");
    let metrics_port = metrics
        .local_addr()
        .expect("metrics listener address")
        .port();

    (
        BootListenerBindings::prebound(http, metrics, None),
        http_port,
        metrics_port,
    )
}

fn assert_loopback_port_released(port: u16) {
    std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
        .unwrap_or_else(|error| panic!("loopback port {port} was not released: {error}"));
}

async fn wait_for_http_status(port: u16, path: &str, expected_status: u16) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Ok(mut stream) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
            let request =
                format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
            if stream.write_all(request.as_bytes()).await.is_ok() {
                let mut response = Vec::new();
                if stream.read_to_end(&mut response).await.is_ok() {
                    let response = String::from_utf8_lossy(&response);
                    if response.starts_with(&format!("HTTP/1.1 {expected_status}")) {
                        return;
                    }
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{path} did not return {expected_status} before the deadline"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
