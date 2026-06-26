//! Transport-layer test utilities for end-to-end integration tests
//!
//! Provides helpers for testing the complete request-response cycle:
//! Client → TCP/WebSocket → Session → Routing → Domain → Response → Client

use bytes::{BufMut, BytesMut};
use futures_util::{SinkExt, StreamExt};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Once, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::{
    client_async,
    tungstenite::{
        client::IntoClientRequest, handshake::client::Request as WebSocketRequest,
        protocol::Message,
    },
    MaybeTlsStream, WebSocketStream,
};

static TEST_SERVER_SEMAPHORE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
static AUTH_JWT_TEST_CACHE: Once = Once::new();

const TEST_ISSUER: &str = "https://idp.example";
const TEST_AUDIENCE: &str = "fitz-broker";
const TEST_RUNTIME_AUTH_SECRET: &str = "test-secret-key";

fn test_server_semaphore() -> &'static Arc<tokio::sync::Semaphore> {
    TEST_SERVER_SEMAPHORE.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
}

fn init_test_runtime_jwks_cache() {
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
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            false,
            None,
            crate::boot::runtime::StorageMode::Memory,
            crate::domains::stream::StreamStorageLayout::default(),
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    pub async fn start_with_rpc_timeout(
        rpc_request_timeout: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            false,
            Some(rpc_request_timeout),
            crate::boot::runtime::StorageMode::Memory,
            crate::domains::stream::StreamStorageLayout::default(),
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    /// Start a test server with configurable auth mode
    pub async fn start_with_auth(auth_required: bool) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            auth_required,
            None,
            crate::boot::runtime::StorageMode::Memory,
            crate::domains::stream::StreamStorageLayout::default(),
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

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
            crate::domains::stream::StreamStorageLayout::default(),
            origins,
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    pub async fn start_with_stream_storage_layout(
        stream_storage_layout: crate::domains::stream::StreamStorageLayout,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            false,
            None,
            crate::boot::runtime::StorageMode::Memory,
            stream_storage_layout,
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    pub async fn start_with_local_storage(
        db_path: impl Into<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_options(
            false,
            None,
            crate::boot::runtime::StorageMode::LocalDisk {
                db_path: db_path.into(),
            },
            crate::domains::stream::StreamStorageLayout::default(),
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

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
            stream_storage_layout,
            Vec::new(),
            vec![1],
            default_test_route_family_mappings(),
        )
        .await
    }

    async fn start_with_options(
        auth_required: bool,
        rpc_request_timeout: Option<Duration>,
        storage_mode: crate::boot::runtime::StorageMode,
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

        // Boot runtime with test configuration
        let boot_config = crate::boot::BootConfig {
            bind_addr: "127.0.0.1".to_string(),
            tcp_port: tcp_addr.port(),
            tcp_enabled: true,
            http_port: ws_addr.port(), // Use discovered WS port
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
            storage_memtable: crate::boot::runtime::StorageMemtableConfig::Auto,
            queue_write_policy: crate::boot::runtime::QueueWritePolicy::Fast,
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
        let server_write_options = boot_config.server_write_options();
        let queue_write_options = boot_config.queue_write_options();
        let domains = crate::boot::domains::setup(
            &router,
            &store,
            &runtime.admin_read_model(),
            crate::boot::domains::DomainSetupOptions {
                server_write_options,
                queue_write_options,
                queue_fast_flush_interval: boot_config.queue_fast_flush_interval(),
                request_sync_write_options: boot_config.request_sync_write_options(),
                rpc_request_timeout,
                stream_storage_layout: boot_config.stream_storage_layout,
            },
        )?;
        runtime.attach_domains(Arc::new(domains));

        // Mark domains ready
        runtime.mark_domains_ready();

        // Step 4: Start TCP listener with pre-bound socket (eliminates port race)
        let tcp_handle = crate::api::handlers::spawn_tcp_listener_with_bound_socket(
            tcp_socket,
            ingress.clone(),
            ingress_config.clone(),
            runtime.clone(),
        )?;

        // Step 5: Start HTTP/WebSocket listener with pre-bound socket
        let ws_handle = crate::api::handlers::spawn_http_listener_with_bound_socket(
            ws_socket,
            ingress.clone(),
            ingress_config.clone(),
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
                "TCP readiness wait failed: {}",
                e
            ))) as Box<dyn std::error::Error>
        })?;
        ws_ready_rx.await.map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "WebSocket readiness wait failed: {}",
                e
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
    pub async fn connect(&self) -> Result<TestClient, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(self.tcp_addr).await?;
        stream.set_nodelay(true)?;
        Ok(TestClient { stream })
    }

    /// Connect to the test server via WebSocket
    pub async fn connect_ws(&self) -> Result<TestWebSocketClient, Box<dyn std::error::Error>> {
        let url = format!("ws://{}/", self.ws_addr);
        TestWebSocketClient::connect(&url).await
    }

    pub async fn connect_ws_with_origin(
        &self,
        origin: &str,
    ) -> Result<TestWebSocketClient, Box<dyn std::error::Error>> {
        let url = format!("ws://{}/", self.ws_addr);
        TestWebSocketClient::connect_with_origin(&url, origin).await
    }

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

    pub async fn websocket_upgrade_status_with_origin_headers(
        &self,
        origins: &[&str],
    ) -> Result<u16, Box<dyn std::error::Error>> {
        let mut stream = TcpStream::connect(self.ws_addr).await?;
        stream.set_nodelay(true)?;
        let origin_headers = origins
            .iter()
            .map(|origin| format!("Origin: {origin}\r\n"))
            .collect::<String>();
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
                format!("timed out waiting for {}", description),
            )) as Box<dyn std::error::Error>
        })
    }

    pub async fn wait_for_authenticated_sessions(
        &self,
        expected_at_least: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.wait_for_condition("authenticated sessions", |runtime| {
            runtime.authenticated_session_count() >= expected_at_least
        })
        .await
    }

    pub async fn wait_for_session_count(
        &self,
        expected: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.wait_for_condition("session count", |runtime| {
            runtime.session_count() == expected
        })
        .await
    }

    pub async fn wait_for_route_count(
        &self,
        expected: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.wait_for_condition("registered route count", |runtime| {
            runtime.registered_route_count() == expected
        })
        .await
    }

    pub fn force_schedule_scan_for_tests(
        &self,
        ready_count: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let domains_guard = self.runtime.domains.read();
        let domains = domains_guard.as_ref().ok_or_else(|| {
            Box::new(std::io::Error::other("domain handles unavailable"))
                as Box<dyn std::error::Error>
        })?;

        domains.schedule.force_due_scan_for_tests(ready_count);
        Ok(())
    }

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

        let _ = tcp_shutdown.send(());
        let _ = ws_shutdown.send(());

        let wait_for_listener = async |name, join: tokio::task::JoinHandle<()>| {
            timeout(Duration::from_secs(6), join)
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
        if let Some(ingress) = runtime.detach_ingress() {
            ingress
                .close_all_sessions(crate::session::CloseReason::ServerClose(
                    "test server shutdown".to_string(),
                ))
                .await;
            drop(ingress);
        }
        runtime.router().clear();
        drop(domains);
        drop(runtime);
        drop(permit);
        let store = Arc::try_unwrap(store).map_err(|store| {
            format!(
                "Midge shutdown blocked by {} leftover engine references",
                Arc::strong_count(&store)
            )
        })?;
        store
            .shutdown()
            .map_err(|error| format!("Midge shutdown failed: {}", error))?;
        Ok(())
    }
}

/// Test client for sending raw protocol frames
pub struct TestClient {
    stream: TcpStream,
}

impl TestClient {
    /// Create a client by connecting to an address
    pub async fn new(addr: SocketAddr) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }

    /// Send a length-prefixed frame (TCP protocol)
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        // Write length prefix (u32 BE)
        let len = frame.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(frame).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Receive a length-prefixed frame with timeout
    pub async fn recv_frame(
        &mut self,
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let recv_future = async {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            self.stream.read_exact(&mut len_buf).await?;
            let len = u32::from_be_bytes(len_buf) as usize;

            // Read frame
            let mut frame = vec![0u8; len];
            self.stream.read_exact(&mut frame).await?;
            Ok::<Vec<u8>, std::io::Error>(frame)
        };

        timeout(Duration::from_millis(timeout_ms), recv_future)
            .await
            .map_err(|_| "timeout waiting for response".to_string())?
            .map_err(|e| e.into())
    }

    /// Send a frame and wait for response
    pub async fn request(
        &mut self,
        frame: &[u8],
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.send_frame(frame).await?;
        self.recv_frame(timeout_ms).await
    }

    /// Gracefully close the TCP client connection.
    pub async fn close(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.stream.shutdown().await?;
        Ok(())
    }
}

/// Test client for WebSocket connections
pub struct TestWebSocketClient {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pending_frames: VecDeque<Message>,
}

impl TestWebSocketClient {
    /// Connect to a WebSocket server
    pub async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let request = url.into_client_request()?;
        Self::connect_request(request).await
    }

    pub async fn connect_with_origin(
        url: &str,
        origin: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut request = url.into_client_request()?;
        request.headers_mut().insert("Origin", origin.parse()?);
        Self::connect_request(request).await
    }

    async fn connect_request(
        request: WebSocketRequest,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let uri = request.uri();
        let host = uri.host().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "websocket url missing host",
            )
        })?;
        let port = uri.port_u16().unwrap_or(80);
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        let (ws_stream, _response) = client_async(request, MaybeTlsStream::Plain(stream)).await?;
        Ok(Self {
            ws: ws_stream,
            pending_frames: VecDeque::new(),
        })
    }

    /// Send a WebSocket binary frame (no length prefix - handled by WebSocket protocol)
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.ws.send(Message::Binary(frame.to_vec().into())).await?;
        Ok(())
    }

    /// Receive a WebSocket binary frame with timeout
    pub async fn recv_frame(
        &mut self,
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let recv_future = async {
            loop {
                // Check if we have pending frames from previous recv() calls
                // This is a fast synchronous check that avoids async overhead
                while let Some(msg) = self.pending_frames.pop_front() {
                    match msg {
                        Message::Binary(data) => return Ok(data.to_vec()),
                        Message::Ping(_) | Message::Pong(_) => continue,
                        Message::Close(_) => return Err("WebSocket closed".into()),
                        Message::Text(_) => continue,
                        Message::Frame(_) => continue,
                    }
                }

                // Pending buffer empty, await next message from WebSocket stream
                match self.ws.next().await {
                    Some(Ok(msg)) => {
                        match msg {
                            Message::Binary(data) => return Ok(data.to_vec()),
                            Message::Ping(_) | Message::Pong(_) => {
                                // Filter out control frames, try next message
                                continue;
                            }
                            Message::Close(_) => {
                                return Err("WebSocket closed".into());
                            }
                            Message::Text(_) => {
                                // Filter out text frames, try next message
                                continue;
                            }
                            Message::Frame(_) => {
                                // Filter out raw frames, try next message
                                continue;
                            }
                        }
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err("WebSocket stream ended".into()),
                }
            }
        };

        timeout(Duration::from_millis(timeout_ms), recv_future)
            .await
            .map_err(|_| "timeout waiting for response".to_string())?
            .map_err(|e: Box<dyn std::error::Error>| e)
    }

    /// Send a frame and wait for response
    pub async fn request(
        &mut self,
        frame: &[u8],
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.send_frame(frame).await?;
        self.recv_frame(timeout_ms).await
    }

    /// Gracefully close the websocket client connection.
    pub async fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.ws.close(None).await?;
        Ok(())
    }
}

/// TLV encoder for building protocol frames
pub struct TlvFrameBuilder {
    buf: BytesMut,
}

impl TlvFrameBuilder {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::new(),
        }
    }

    /// Encode a TLV field: [message_type: u8 or ESCAPE+u16 BE][length: u16 BE][value: bytes]
    pub fn encode_field(&mut self, msg_type: u16, value: &[u8]) {
        // MessageType encoding:
        // - If msg_type <= 254: single byte
        // - If msg_type > 254: [0xFF escape][msg_type as u16 BE]
        const ESCAPE_MARKER: u8 = 0xFF;
        const MAX_SINGLE_BYTE: u16 = 254;

        if msg_type <= MAX_SINGLE_BYTE {
            self.buf.put_u8(msg_type as u8);
        } else {
            self.buf.put_u8(ESCAPE_MARKER);
            self.buf.put_slice(&msg_type.to_be_bytes());
        }

        // Length is u16 BE (max 65535 bytes)
        if value.len() > 65535 {
            panic!("TLV value too large: {} bytes", value.len());
        }
        self.buf.put_slice(&(value.len() as u16).to_be_bytes());

        // Value
        self.buf.put_slice(value);
    }

    /// Build the final frame
    pub fn build(self) -> Vec<u8> {
        self.buf.to_vec()
    }
}

/// TLV decoder for parsing protocol frames
pub struct TlvFrameParser<'a> {
    buf: &'a [u8],
    offset: usize,
}

impl<'a> TlvFrameParser<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, offset: 0 }
    }

    /// Parse next TLV field without copying the payload.
    pub fn next_field_ref(&mut self) -> Option<(u16, &'a [u8])> {
        const ESCAPE_MARKER: u8 = 0xFF;

        if self.offset >= self.buf.len() {
            return None;
        }

        // Parse msg_type
        let msg_type = if self.buf[self.offset] == ESCAPE_MARKER {
            if self.offset + 3 > self.buf.len() {
                return None;
            }
            let mt = u16::from_be_bytes([self.buf[self.offset + 1], self.buf[self.offset + 2]]);
            self.offset += 3;
            mt
        } else {
            let mt = self.buf[self.offset] as u16;
            self.offset += 1;
            mt
        };

        // Parse length (u16 BE)
        if self.offset + 2 > self.buf.len() {
            return None;
        }
        let len = u16::from_be_bytes([self.buf[self.offset], self.buf[self.offset + 1]]) as usize;
        self.offset += 2;

        // Parse value
        if self.offset + len > self.buf.len() {
            return None;
        }
        let value = &self.buf[self.offset..self.offset + len];
        self.offset += len;

        Some((msg_type, value))
    }

    /// Parse next TLV field into an owned buffer.
    pub fn next_field(&mut self) -> Option<(u16, Vec<u8>)> {
        self.next_field_ref()
            .map(|(msg_type, value)| (msg_type, value.to_vec()))
    }

    /// Parse all fields
    pub fn parse_all(&mut self) -> Vec<(u16, Vec<u8>)> {
        let mut fields = Vec::new();
        while let Some(field) = self.next_field() {
            fields.push(field);
        }
        fields
    }
}

impl Default for TlvFrameBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Build CONNECT message (msg_type 1).
/// The legacy route argument is ignored; CONNECT carries only the JWT payload.
pub fn build_connect_frame(_realm: &str, jwt_token: &str) -> Vec<u8> {
    // CONNECT frame: [msg_type: 1][length: u16 BE][JWT string bytes]
    // Server expects JWT as plain UTF-8 string, no additional structure
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(1, jwt_token.as_bytes()); // msg_type 1 = CONNECT
    builder.build()
}

/// Generate a provider-shaped test JWT for a single partition string.
/// Uses JWKS-mode token shape signed with the shared test issuer secret.
/// Token is valid for 1 hour from now
/// Emits `tid` plus top-level `permissions`
pub fn generate_test_jwt(realm: &str) -> String {
    generate_test_jwt_for_family(realm, 1)
}

pub fn generate_test_jwt_for_family(realm: &str, _route_family: u32) -> String {
    init_test_runtime_jwks_cache();
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        iss: String,
        aud: String,
        tid: String,
        sub: String,
        exp: i64,
        iat: i64,
        permissions: Vec<String>,
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let claims = Claims {
        iss: TEST_ISSUER.to_string(),
        aud: TEST_AUDIENCE.to_string(),
        tid: realm.to_string(),
        sub: realm.to_string(),
        exp: now + 3600, // Valid for 1 hour
        iat: now,
        permissions: vec![
            format!("kv://{}/**#*", realm), // Full KV access for this realm
            format!("queue://{}/**#*", realm),
            format!("notice://{}/**#*", realm),
            format!("stream://{}/**#*", realm),
            format!("rpc://{}/**#*", realm),
            format!("lease://{}/**#*", realm),
            format!("schedule://{}/**#*", realm),
        ],
    };

    let mut header = Header::new(Algorithm::HS256);
    header.typ = Some("JWT".to_string());

    encode(
        &header,
        &claims,
        &EncodingKey::from_secret(TEST_RUNTIME_AUTH_SECRET.as_bytes()),
    )
    .unwrap()
}

/// Generate expired JWT (for testing rejection)
pub fn generate_expired_jwt(realm: &str) -> String {
    init_test_runtime_jwks_cache();
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        iss: String,
        aud: String,
        tid: String,
        sub: String,
        exp: i64,
        iat: i64,
        permissions: Vec<String>,
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let claims = Claims {
        iss: TEST_ISSUER.to_string(),
        aud: TEST_AUDIENCE.to_string(),
        tid: realm.to_string(),
        sub: realm.to_string(),
        exp: now - 3600, // Expired 1 hour ago
        iat: now - 7200,
        permissions: vec![format!("kv://{}/**#*", realm)],
    };

    let header = Header::new(Algorithm::HS256);

    encode(
        &header,
        &claims,
        &EncodingKey::from_secret(TEST_RUNTIME_AUTH_SECRET.as_bytes()),
    )
    .unwrap()
}

/// Generate JWT with invalid signature (for testing rejection)
pub fn generate_invalid_signature_jwt(realm: &str) -> String {
    init_test_runtime_jwks_cache();
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        iss: String,
        aud: String,
        tid: String,
        sub: String,
        exp: i64,
        iat: i64,
        permissions: Vec<String>,
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let claims = Claims {
        iss: TEST_ISSUER.to_string(),
        aud: TEST_AUDIENCE.to_string(),
        tid: realm.to_string(),
        sub: realm.to_string(),
        exp: now + 3600,
        iat: now,
        permissions: vec![format!("kv://{}/**#*", realm)],
    };

    let header = Header::new(Algorithm::HS256);

    // Use wrong secret to create invalid signature
    encode(
        &header,
        &claims,
        &EncodingKey::from_secret("wrong-secret-key".as_bytes()),
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn websocket_upgrade_status_for_addr(
        addr: SocketAddr,
    ) -> Result<u16, Box<dyn std::error::Error>> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let request = format!(
            "GET / HTTP/1.1\r\n\
             Host: {}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n",
            addr
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

    #[test]
    fn should_encode_tlv_frame() {
        // Arrange
        let mut builder = TlvFrameBuilder::new();
        builder.encode_field(100, b"test_value");

        // Act
        let frame = builder.build();

        // Assert
        assert!(frame.len() >= 3 + b"test_value".len());
        assert_eq!(frame[0], 100); // msg_type
        assert_eq!(frame[1], 0);
        assert_eq!(frame[2], b"test_value".len() as u8);
        assert_eq!(&frame[3..], b"test_value");
    }

    #[test]
    fn should_decode_tlv_frame() {
        // Arrange
        let mut builder = TlvFrameBuilder::new();
        builder.encode_field(100, b"test_value");
        builder.encode_field(200, b"another_value");
        let frame = builder.build();

        // Act
        let mut parser = TlvFrameParser::new(&frame);
        let fields = parser.parse_all();

        // Assert
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, 100);
        assert_eq!(fields[0].1, b"test_value");
        assert_eq!(fields[1].0, 200);
        assert_eq!(fields[1].1, b"another_value");
    }

    #[test]
    fn should_build_connect_frame() {
        // Arrange
        let realm = "test-realm";
        let jwt = "fake-jwt-token";

        // Act
        let frame = build_connect_frame(realm, jwt);

        // Assert
        assert!(!frame.is_empty());
        assert_eq!(frame[0], 1); // msg_type 1 (CONNECT)
    }

    #[test]
    fn should_generate_valid_jwt() {
        // Arrange
        let realm = "test-realm";

        // Act
        let jwt = generate_test_jwt(realm);

        // Assert
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "JWT should have header.payload.signature format"
        );
    }

    #[tokio::test]
    async fn should_shutdown_with_active_tcp_and_websocket_sessions() {
        // Arrange
        let server = TestServer::start().await.expect("start test server");
        let _tcp = server.connect().await.expect("connect tcp client");
        let _websocket = server.connect_ws().await.expect("connect websocket client");
        server
            .wait_for_session_count(2)
            .await
            .expect("wait for active sessions");

        // Act
        let result = server.shutdown().await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_accept_websocket_upgrade_given_allowed_origin() {
        // Arrange
        let server = TestServer::start_with_ws_allowed_origins(&["https://app.example.com"])
            .await
            .expect("start test server");

        // Act
        let _websocket = server
            .connect_ws_with_origin("https://app.example.com")
            .await
            .expect("connect websocket");

        // Assert
        server
            .wait_for_session_count(1)
            .await
            .expect("wait for websocket session");
    }

    #[tokio::test]
    async fn should_reject_websocket_upgrade_given_disallowed_origin() {
        // Arrange
        let server = TestServer::start_with_ws_allowed_origins(&["https://app.example.com"])
            .await
            .expect("start test server");

        // Act
        let status = server
            .websocket_upgrade_status(Some("https://evil.example.com"))
            .await
            .expect("read websocket status");

        // Assert
        assert_eq!(status, 403);
        assert_eq!(server.runtime.session_count(), 0);
    }

    #[tokio::test]
    async fn should_allow_websocket_upgrade_given_missing_origin_when_origins_configured() {
        // Arrange
        let server = TestServer::start_with_ws_allowed_origins(&["https://app.example.com"])
            .await
            .expect("start test server");

        // Act
        let status = server
            .websocket_upgrade_status(None)
            .await
            .expect("read websocket status");

        // Assert
        assert_eq!(status, 101);
    }

    #[tokio::test]
    async fn should_reject_websocket_upgrade_given_duplicate_origin_headers() {
        // Arrange
        let server = TestServer::start_with_ws_allowed_origins(&["https://app.example.com"])
            .await
            .expect("start test server");

        // Act
        let status = server
            .websocket_upgrade_status_with_origin_headers(&[
                "https://app.example.com",
                "https://app.example.com",
            ])
            .await
            .expect("read websocket status");

        // Assert
        assert_eq!(status, 403);
        assert_eq!(server.runtime.session_count(), 0);
    }

    #[tokio::test]
    async fn should_allow_loopback_websocket_upgrade_without_origin_config() {
        // Arrange
        let server = TestServer::start().await.expect("start test server");

        // Act
        let status = server
            .websocket_upgrade_status(None)
            .await
            .expect("read websocket status");

        // Assert
        assert_eq!(status, 101);
    }

    #[tokio::test]
    async fn should_reject_websocket_upgrade_before_data_plane_readiness() {
        // Arrange
        let ws_socket = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind websocket listener");
        let ws_addr = ws_socket.local_addr().expect("websocket listener addr");
        let boot_config = crate::boot::BootConfig::with_memory_storage()
            .with_auth_config(crate::auth::AuthConfig::Disabled)
            .with_bind_addr("127.0.0.1".to_string())
            .with_http_port(ws_addr.port());
        let (_, ingress, ingress_config, runtime) =
            crate::boot::runtime::init(&boot_config).expect("initialize runtime");
        runtime.mark_auth_config_ready();
        let handle = crate::api::handlers::spawn_http_listener_with_bound_socket(
            ws_socket,
            ingress,
            ingress_config,
            runtime,
            boot_config.ws_allowed_origins.clone(),
        )
        .expect("spawn websocket listener");
        handle.ready.await.expect("websocket listener ready");

        // Act
        let status = websocket_upgrade_status_for_addr(ws_addr)
            .await
            .expect("read websocket status");

        // Assert
        assert_eq!(status, 503);
        let _ = handle.shutdown.send(());
        handle.join.await.expect("websocket listener shutdown");
    }

    #[tokio::test]
    async fn should_reject_tcp_session_before_data_plane_readiness() {
        // Arrange
        let tcp_socket = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind tcp listener");
        let tcp_addr = tcp_socket.local_addr().expect("tcp listener addr");
        let boot_config = crate::boot::BootConfig::with_memory_storage()
            .with_auth_config(crate::auth::AuthConfig::Disabled)
            .with_bind_addr("127.0.0.1".to_string())
            .with_tcp_port(tcp_addr.port());
        let (_, ingress, ingress_config, runtime) =
            crate::boot::runtime::init(&boot_config).expect("initialize runtime");
        runtime.mark_auth_config_ready();
        let handle = crate::api::handlers::spawn_tcp_listener_with_bound_socket(
            tcp_socket,
            ingress,
            ingress_config,
            runtime.clone(),
        )
        .expect("spawn tcp listener");
        handle.ready.await.expect("tcp listener ready");

        // Act
        let _stream = TcpStream::connect(tcp_addr)
            .await
            .expect("connect tcp socket");
        tokio::task::yield_now().await;

        // Assert
        assert_eq!(runtime.session_count(), 0);
        let _ = handle.shutdown.send(());
        handle.join.await.expect("tcp listener shutdown");
    }

    #[tokio::test]
    async fn should_reject_websocket_upgrade_when_broker_is_draining() {
        // Arrange
        let server = TestServer::start_with_ws_allowed_origins(&["https://app.example.com"])
            .await
            .expect("start test server");
        server.runtime.begin_drain();

        // Act
        let status = server
            .websocket_upgrade_status(Some("https://app.example.com"))
            .await
            .expect("read websocket status");

        // Assert
        assert_eq!(status, 503);
        assert_eq!(server.runtime.session_count(), 0);
    }

    #[tokio::test]
    async fn should_reject_tcp_session_when_broker_is_draining() {
        // Arrange
        let server = TestServer::start().await.expect("start test server");
        server.runtime.begin_drain();

        // Act
        let _stream = TcpStream::connect(server.tcp_addr)
            .await
            .expect("connect tcp socket");
        tokio::task::yield_now().await;

        // Assert
        assert_eq!(server.runtime.session_count(), 0);
    }
}
