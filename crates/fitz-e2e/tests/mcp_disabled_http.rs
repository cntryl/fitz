use fitz::api::handlers::spawn_http_listener_with_bound_socket;
use fitz::auth::AuthConfig;
use fitz::boot::{runtime, BootConfig};
use reqwest::{Client, StatusCode};
use serde_json::Value;
use serial_test::serial;
use std::ffi::OsString;
use std::time::Duration;
use tokio::net::TcpListener;

struct EnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
    fn change(key: &'static str, value: Option<&str>) -> Self {
        let previous = std::env::var_os(key);
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

#[tokio::test]
#[serial]
async fn should_keep_mcp_disabled_by_default_on_the_existing_http_listener() {
    // Arrange
    let _mcp = EnvGuard::change("FITZ_MCP_HTTP_ENABLED", None);
    let _admin = EnvGuard::change("FITZ_ADMIN_AUTH_MODE", Some("open"));
    let _families = EnvGuard::change("FITZ_ADMIN_ROUTE_FAMILIES", Some("*"));
    let _password = EnvGuard::change("FITZ_ROOT_PASSWORD", None);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config = BootConfig::with_memory_storage()
        .with_auth_config(AuthConfig::Disabled)
        .with_bind_addr("127.0.0.1".into())
        .with_http_port(address.port());
    let (_, ingress, ingress_config, runtime) = runtime::init(&config).unwrap();
    runtime.mark_storage_ready();
    runtime.mark_domains_ready();
    runtime.mark_auth_config_ready();
    runtime.mark_startup_complete();
    let handle = spawn_http_listener_with_bound_socket(
        listener,
        ingress,
        &ingress_config,
        runtime,
        config.ws_allowed_origins,
    )
    .unwrap();
    handle.ready.await.unwrap();
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(0)
        .build()
        .unwrap();
    let base = format!("http://{address}");

    // Act
    let mcp = client.get(format!("{base}/mcp")).send().await.unwrap();
    let metadata = client
        .get(format!("{base}/.well-known/oauth-protected-resource/mcp"))
        .send()
        .await
        .unwrap();
    let health = client.get(format!("{base}/healthz")).send().await.unwrap();
    let stats = client
        .get(format!("{base}/api/v1/stats"))
        .send()
        .await
        .unwrap();
    let health_status = health.status();
    let health: Value = serde_json::from_slice(&health.bytes().await.unwrap()).unwrap();
    let stats_status = stats.status();
    let stats: Value = serde_json::from_slice(&stats.bytes().await.unwrap()).unwrap();
    let mcp_status = mcp.status();
    let metadata_status = metadata.status();
    drop((mcp, metadata, client));
    handle.shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle.join)
        .await
        .expect("production HTTP listener reaped")
        .unwrap();

    // Assert
    assert_eq!(mcp_status, StatusCode::NOT_FOUND);
    assert_eq!(metadata_status, StatusCode::NOT_FOUND);
    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(health["status"], "ready");
    assert_eq!(stats_status, StatusCode::OK);
    assert!(stats["broker"].is_object());
    assert!(stats["domains"].is_object());
}
