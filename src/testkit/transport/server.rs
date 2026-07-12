// Transport-layer test utilities for end-to-end integration tests
//
// Provides helpers for testing the complete request-response cycle:
// Client → TCP/WebSocket → Session → Routing → Domain → Response → Client

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Once, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};

use super::{
    TestClient, TestWebSocketClient, TEST_AUDIENCE, TEST_ISSUER, TEST_RUNTIME_AUTH_SECRET,
};

static TEST_SERVER_SEMAPHORE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
static AUTH_JWT_TEST_CACHE: Once = Once::new();

const STORE_SHUTDOWN_RELEASE_TIMEOUT: Duration = Duration::from_secs(2);
const STORE_SHUTDOWN_RELEASE_POLL_INTERVAL: Duration = Duration::from_millis(10);

fn test_server_semaphore() -> &'static Arc<tokio::sync::Semaphore> {
    TEST_SERVER_SEMAPHORE.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
}

pub(super) fn init_test_runtime_jwks_cache() {
    AUTH_JWT_TEST_CACHE.call_once(|| {
        use base64::Engine;

        let jwks_url = crate::auth::derive_jwks_url_from_issuer(TEST_ISSUER).unwrap();
        let k_b64 =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(TEST_RUNTIME_AUTH_SECRET);
        let jwks = serde_json::json!({
            "keys": [
                {
                    "kty": "oct",
                    "kid": "",
                    "k": k_b64,
                }
            ]
        })
        .to_string();

        crate::auth::cache_jwks_from_json(&jwks_url, &jwks).unwrap();
    });
}

async fn wait_for_exclusive_store(
    mut store: Arc<cntryl_midge::Engine>,
) -> Result<cntryl_midge::Engine, String> {
    let deadline = Instant::now() + STORE_SHUTDOWN_RELEASE_TIMEOUT;
    loop {
        match Arc::try_unwrap(store) {
            Ok(engine) => return Ok(engine),
            Err(shared_store) => {
                if Instant::now() >= deadline {
                    return Err(format!(
                        "Midge shutdown blocked by {} leftover engine references",
                        Arc::strong_count(&shared_store)
                    ));
                }
                store = shared_store;
                sleep(STORE_SHUTDOWN_RELEASE_POLL_INTERVAL).await;
            }
        }
    }
}

fn default_test_route_family_mappings() -> Vec<(String, u32)> {
    vec![("test-realm".to_string(), 1), ("acme".to_string(), 1)]
}

/// Test server that starts Fitz on random available ports (TCP + WebSocket)
pub struct TestServer {
    pub tcp_addr: SocketAddr,
    pub ws_addr: SocketAddr,
    pub runtime: Arc<crate::boot::Runtime>,
    store: Arc<cntryl_midge::Engine>,
    _tcp_shutdown: tokio::sync::oneshot::Sender<()>,
    _ws_shutdown: tokio::sync::oneshot::Sender<()>,
    _tcp_join: tokio::task::JoinHandle<()>,
    _ws_join: tokio::task::JoinHandle<()>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl TestServer {
    /// Start a test server with auth disabled (backward compatible)
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime or listeners cannot be initialized.
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            false,
            None,
            crate::boot::runtime::StorageMode::Memory,
            false,
            crate::domains::stream::StreamStorageLayout::default(),
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    /// Start a benchmark-only memory server with capacity for fixed-duration write loops.
    ///
    /// Ordinary tests keep Midge's default memory configuration. Tier 4 write benchmarks
    /// opt into this fixture so their signal is not capped by a benchmark-induced write stall.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime or listeners cannot be initialized.
    pub async fn start_with_write_heavy_memory() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            false,
            None,
            crate::boot::runtime::StorageMode::Memory,
            true,
            crate::domains::stream::StreamStorageLayout::default(),
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    /// # Errors
    ///
    /// Returns an error if the runtime or listeners cannot be initialized.
    pub async fn start_with_rpc_timeout(
        rpc_request_timeout: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            false,
            Some(rpc_request_timeout),
            crate::boot::runtime::StorageMode::Memory,
            false,
            crate::domains::stream::StreamStorageLayout::default(),
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    /// Start a test server with configurable auth mode
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime or listeners cannot be initialized.
    pub async fn start_with_auth(auth_required: bool) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            auth_required,
            None,
            crate::boot::runtime::StorageMode::Memory,
            false,
            crate::domains::stream::StreamStorageLayout::default(),
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    /// # Errors
    ///
    /// Returns an error if the runtime or listeners cannot be initialized.
    pub async fn start_with_auth_route_families<I, S>(
        route_families: Vec<u32>,
        route_family_mappings: I,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        I: IntoIterator<Item = (S, u32)>,
        S: Into<String>,
    {
        Self::start_with_options(
            true,
            None,
            crate::boot::runtime::StorageMode::Memory,
            false,
            crate::domains::stream::StreamStorageLayout::default(),
            Vec::new(),
            route_families,
            route_family_mappings
                .into_iter()
                .map(|(identity, family)| (identity.into(), family))
                .collect(),
        )
        .await
    }

    /// # Errors
    ///
    /// Returns an error if any origin is invalid or the runtime cannot be initialized.
    pub async fn start_with_ws_allowed_origins(
        origins: &[&str],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let origins = origins
            .iter()
            .map(|origin| {
                crate::api::origin::parse_exact_origin(origin).map_err(|error| {
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
                        as Box<dyn std::error::Error>
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::start_with_options(
            false,
            None,
            crate::boot::runtime::StorageMode::Memory,
            false,
            crate::domains::stream::StreamStorageLayout::default(),
            origins,
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    /// # Errors
    ///
    /// Returns an error if the runtime or listeners cannot be initialized.
    pub async fn start_with_stream_storage_layout(
        stream_storage_layout: crate::domains::stream::StreamStorageLayout,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            false,
            None,
            crate::boot::runtime::StorageMode::Memory,
            false,
            stream_storage_layout,
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    /// # Errors
    ///
    /// Returns an error if the runtime or listeners cannot be initialized.
    pub async fn start_with_local_storage(
        db_path: impl Into<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            false,
            None,
            crate::boot::runtime::StorageMode::LocalDisk {
                db_path: db_path.into(),
            },
            false,
            crate::domains::stream::StreamStorageLayout::default(),
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    /// # Errors
    ///
    /// Returns an error if the runtime or listeners cannot be initialized.
    pub async fn start_with_local_storage_and_stream_layout(
        db_path: impl Into<String>,
        stream_storage_layout: crate::domains::stream::StreamStorageLayout,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            false,
            None,
            crate::boot::runtime::StorageMode::LocalDisk {
                db_path: db_path.into(),
            },
            false,
            stream_storage_layout,
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn start_with_options(
        auth_required: bool,
        rpc_request_timeout: Option<Duration>,
        storage_mode: crate::boot::runtime::StorageMode,
        write_heavy_memory: bool,
        stream_storage_layout: crate::domains::stream::StreamStorageLayout,
        ws_allowed_origins: Vec<crate::api::origin::ExactOrigin>,
        route_families: Vec<u32>,
        route_family_mappings: Vec<(String, u32)>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let permit = test_server_semaphore()
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        // Initialize observability (metrics + tracing) once for tests
        // Safe to call multiple times - will only initialize once
        let _ = crate::observability::try_init_observability();

        if auth_required {
            init_test_runtime_jwks_cache();
        }

        // Find available ports and keep listeners bound to prevent reallocation race
        let tcp_socket = TcpListener::bind("127.0.0.1:0").await?;
        let tcp_addr = tcp_socket.local_addr()?;

        let ws_socket = TcpListener::bind("127.0.0.1:0").await?;
        let ws_addr = ws_socket.local_addr()?;

        // Keep listeners alive - will be passed to spawn functions
        // This prevents the port reallocation race condition where parallel tests
        // could grab the same port between bind() and the spawn functions

        let memory_storage = matches!(&storage_mode, crate::boot::runtime::StorageMode::Memory);

        // Boot runtime with test configuration
        let boot_config = crate::boot::BootConfig {
            bind_addr: "127.0.0.1".to_string(),
            tcp_port: tcp_addr.port(),
            tcp_enabled: true,
            http_port: ws_addr.port(), // Use discovered WS port
            metrics_bind_addr: "127.0.0.1".to_string(),
            metrics_port: 0,
            storage_mode,
            stream_storage_layout,
            auth_required,
            auth_config: if auth_required {
                crate::auth::AuthConfig::jwks(
                    vec![TEST_AUDIENCE.to_string()],
                    vec![crate::auth::JwksIssuerConfig {
                        issuer: TEST_ISSUER.to_string(),
                        jwks_url: crate::auth::derive_jwks_url_from_issuer(TEST_ISSUER).unwrap(),
                    }],
                )
            } else {
                crate::auth::AuthConfig::Disabled
            },
            auth_claims_config: crate::auth::AuthClaimsConfig::default(),
            route_family_resolver: if auth_required {
                crate::auth::RouteFamilyResolverConfig::from_mappings(
                    crate::auth::DEFAULT_ROUTE_FAMILY_CLAIM,
                    route_family_mappings,
                )
            } else {
                crate::auth::RouteFamilyResolverConfig::default()
            },
            route_families,
            max_connections: 1000,
            max_frame_size: 16_777_216, // 16 MB (test config allows larger frames than production 1 MB default)
            channel_capacity: 10_000,
            cloud_durability: crate::boot::runtime::CloudDurabilityMode::Background,
            storage_memtable: if memory_storage && write_heavy_memory {
                // Integration benchmarks intentionally perform many fixed-duration writes;
                // keep the ephemeral test store from stalling before the sample completes.
                crate::boot::runtime::StorageMemtableConfig::Bytes(512 * 1024 * 1024)
            } else {
                crate::boot::runtime::StorageMemtableConfig::Auto
            },
            queue_write_policy: crate::boot::runtime::QueueWritePolicy::Fast,
            queue_write_policy_source: crate::boot::runtime::QueueWritePolicySource::Explicit,
            queue_loss_window_ms: 100,
            queue_loss_window_error: None,
            assume_external_tls: false,
            ws_allowed_origins,
            ws_allowed_origins_error: None,
            drain_grace_seconds: 1,
            drain_close_reason: "test server draining".to_string(),
            drain_config_error: None,
        };

        // Step 1: Initialize storage
        let store = crate::boot::storage::init(&boot_config).await?;

        // Step 2: Initialize runtime
        let (router, ingress, ingress_config, runtime) = crate::boot::runtime::init(&boot_config)?;

        runtime.mark_auth_config_ready();

        // Mark storage ready
        runtime.mark_storage_ready();

        // Step 3: Register domain actors
        let schedule_write_options = boot_config.schedule_write_options();
        let queue_write_options = boot_config.queue_write_options();
        let domains = crate::boot::domains::setup(
            &router,
            &store,
            &runtime.admin_read_model(),
            &crate::boot::domains::DomainSetupOptions {
                route_families: boot_config.route_families.clone(),
                schedule_write_options,
                queue_write_options,
                queue_fast_flush_interval: boot_config.queue_fast_flush_interval(),
                request_sync_write_options: boot_config.request_sync_write_options(),
                rpc_request_timeout,
                stream_storage_layout: boot_config.stream_storage_layout,
            },
        )?;
        runtime.attach_domains(domains);

        // Mark domains ready
        runtime.mark_domains_ready();

        // Step 4: Start TCP listener with pre-bound socket (eliminates port race)
        let tcp_handle = crate::api::handlers::spawn_tcp_listener_with_bound_socket(
            tcp_socket,
            ingress.clone(),
            &ingress_config,
            runtime.clone(),
        )?;

        // Step 5: Start HTTP/WebSocket listener with pre-bound socket
        let ws_handle = crate::api::handlers::spawn_http_listener_with_bound_socket(
            ws_socket,
            ingress.clone(),
            &ingress_config,
            runtime.clone(),
            boot_config.ws_allowed_origins.clone(),
        )?;

        let crate::api::handlers::ListenerHandle {
            ready: tcp_ready_rx,
            shutdown: tcp_shutdown,
            join: tcp_join,
        } = tcp_handle;
        let crate::api::handlers::ListenerHandle {
            ready: ws_ready_rx,
            shutdown: ws_shutdown,
            join: ws_join,
        } = ws_handle;

        // Wait for both listeners to be ready before returning
        // This ensures tests don't connect before accept loops are ready
        tcp_ready_rx.await.map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "TCP readiness wait failed: {e}"
            ))) as Box<dyn std::error::Error>
        })?;
        ws_ready_rx.await.map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "WebSocket readiness wait failed: {e}"
            ))) as Box<dyn std::error::Error>
        })?;

        // Mark startup complete
        runtime.mark_startup_complete();

        // Return the runtime wrapped in Arc
        let runtime_arc = Arc::new(runtime);

        Ok(TestServer {
            tcp_addr,
            ws_addr,
            runtime: runtime_arc,
            store,
            _tcp_shutdown: tcp_shutdown,
            _ws_shutdown: ws_shutdown,
            _tcp_join: tcp_join,
            _ws_join: ws_join,
            _permit: permit,
        })
    }

    /// Connect to the test server via TCP
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP connection cannot be established or configured.
    pub async fn connect(&self) -> Result<TestClient, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(self.tcp_addr).await?;
        stream.set_nodelay(true)?;
        Ok(TestClient { stream })
    }

    /// Connect to the test server via WebSocket
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket connection or handshake fails.
    pub async fn connect_ws(&self) -> Result<TestWebSocketClient, Box<dyn std::error::Error>> {
        let url = format!("ws://{}/", self.ws_addr);
        TestWebSocketClient::connect(&url).await
    }

    /// Connect to the test server via WebSocket with an explicit `Origin` header.
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket connection or handshake fails.
    pub async fn connect_ws_with_origin(
        &self,
        origin: &str,
    ) -> Result<TestWebSocketClient, Box<dyn std::error::Error>> {
        let url = format!("ws://{}/", self.ws_addr);
        TestWebSocketClient::connect_with_origin(&url, origin).await
    }

    /// Attempt a WebSocket upgrade and return the resulting HTTP status code.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket operation, HTTP parsing, or status parsing fails.
    pub async fn websocket_upgrade_status(
        &self,
        origin: Option<&str>,
    ) -> Result<u16, Box<dyn std::error::Error>> {
        match origin {
            Some(origin) => {
                self.websocket_upgrade_status_with_origin_headers(&[origin])
                    .await
            }
            None => self.websocket_upgrade_status_with_origin_headers(&[]).await,
        }
    }

    /// Attempt a WebSocket upgrade with explicit `Origin` headers.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket operation, HTTP parsing, or status parsing fails.
    pub async fn websocket_upgrade_status_with_origin_headers(
        &self,
        origins: &[&str],
    ) -> Result<u16, Box<dyn std::error::Error>> {
        let mut stream = TcpStream::connect(self.ws_addr).await?;
        stream.set_nodelay(true)?;
        let mut origin_headers = String::new();
        for origin in origins {
            let _ = write!(origin_headers, "Origin: {origin}\r\n");
        }
        let request = format!(
            "GET / HTTP/1.1\r\n\
             Host: {}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Version: 13\r\n\
             {origin_headers}\r\n",
            self.ws_addr
        );
        stream.write_all(request.as_bytes()).await?;
        let mut response = [0_u8; 1024];
        let bytes_read = stream.read(&mut response).await?;
        let status_line = std::str::from_utf8(&response[..bytes_read])?
            .lines()
            .next()
            .ok_or_else(|| std::io::Error::other("missing HTTP status line"))?;
        let status = status_line
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| std::io::Error::other("missing HTTP status code"))?
            .parse::<u16>()?;
        Ok(status)
    }

    async fn wait_for_condition<F>(
        &self,
        description: &str,
        mut predicate: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(&crate::boot::Runtime) -> bool,
    {
        timeout(Duration::from_secs(5), async {
            loop {
                if predicate(self.runtime.as_ref()) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("timed out waiting for {description}"),
            )) as Box<dyn std::error::Error>
        })
    }

    /// Wait until at least the requested number of sessions are authenticated.
    ///
    /// # Errors
    ///
    /// Returns an error if the wait times out.
    pub async fn wait_for_authenticated_sessions(
        &self,
        expected_at_least: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.wait_for_condition("authenticated sessions", |runtime| {
            runtime.authenticated_session_count() >= expected_at_least
        })
        .await
    }

    /// Wait until the runtime reports the requested total session count.
    ///
    /// # Errors
    ///
    /// Returns an error if the wait times out.
    pub async fn wait_for_session_count(
        &self,
        expected: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.wait_for_condition("session count", |runtime| {
            runtime.session_count() == expected
        })
        .await
    }

    /// Wait until the runtime reports the requested active KV transaction count.
    ///
    /// # Errors
    ///
    /// Returns an error if the wait times out.
    pub async fn wait_for_kv_transaction_count(
        &self,
        expected: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.wait_for_condition("KV transaction count", |runtime| {
            runtime.kv_list_transactions(None).len() == expected
        })
        .await
    }

    /// Wait until the runtime lease admin snapshot reports the requested active lease count.
    ///
    /// # Errors
    ///
    /// Returns an error if the wait times out.
    pub async fn wait_for_lease_count(
        &self,
        expected: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.wait_for_condition("lease count", |runtime| {
            runtime.lease_list_leases(None).len() == expected
        })
        .await
    }

    /// Wait until the runtime reports the requested registered route count.
    ///
    /// # Errors
    ///
    /// Returns an error if the wait times out.
    pub async fn wait_for_route_count(
        &self,
        expected: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.wait_for_condition("registered route count", |runtime| {
            runtime.registered_route_count() == expected
        })
        .await
    }

    /// Force a schedule due-scan in tests without waiting for the normal loop.
    ///
    /// # Errors
    ///
    /// Returns an error if the domain handles are unavailable.
    pub fn force_schedule_scan_for_tests(
        &self,
        ready_count: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let domains_guard = self.runtime.domains.read();
        let domains = domains_guard.as_ref().ok_or_else(|| {
            Box::new(std::io::Error::other("domain handles unavailable"))
                as Box<dyn std::error::Error>
        })?;

        domains.schedule_force_due_scan_for_tests(ready_count);
        Ok(())
    }

    /// Shut down listeners and background tasks owned by the test server.
    ///
    /// # Errors
    ///
    /// Returns an error if listener shutdown or task joins fail.
    pub async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        let TestServer {
            _tcp_shutdown: tcp_shutdown,
            _ws_shutdown: ws_shutdown,
            _tcp_join: tcp_join,
            _ws_join: ws_join,
            runtime,
            store,
            _permit: permit,
            ..
        } = self;

        runtime.begin_shutdown();

        if let Some(ingress) = runtime.detach_ingress() {
            ingress
                .close_all_sessions(crate::session::CloseReason::ServerClose(
                    "test server shutdown".to_string(),
                ))
                .await;
            drop(ingress);
        }

        let _ = tcp_shutdown.send(());
        let _ = ws_shutdown.send(());

        let wait_for_listener = async |name, join: tokio::task::JoinHandle<()>| {
            timeout(Duration::from_secs(6), join)
                .await
                .map_err(|_| format!("{name} listener shutdown timed out"))?
                .map_err(|error| format!("{name} listener join failed: {error}"))
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
        runtime.router().clear();
        drop(domains);
        drop(runtime);
        drop(permit);
        let store = wait_for_exclusive_store(store).await?;
        store
            .shutdown()
            .map_err(|error| format!("Midge shutdown failed: {error}"))?;
        Ok(())
    }
}
