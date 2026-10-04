use super::*;
use crate::api::admin::auth::{AdminPrincipal, AdminRouteFamilyAccess};
use crate::boot::Runtime;
use crate::control::admin::read_model::AdminReadModel;
use crate::runtime::Router;
use crate::session::permissions::SessionPermissions;
use bytes::Bytes;
use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use rmcp::model::{
    CallToolRequestParams, ClientConfig, GetPromptRequestParams, Implementation, ProtocolVersion,
};
use rmcp::service::{ClientLifecycleMode, ClientServiceExt};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::StreamableHttpClientTransport;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[test]
fn should_normalize_default_tls_port_for_allowed_host_authority() {
    // Arrange
    let host_without_port = parse_authority("mcp.example.test");
    let host_with_port = parse_authority("mcp.example.test:443");

    // Act
    let alternate_port = parse_authority("mcp.example.test:8443");

    // Assert
    assert_eq!(host_without_port, Some(("mcp.example.test".into(), 443)));
    assert_eq!(host_with_port, Some(("mcp.example.test".into(), 443)));
    assert_eq!(alternate_port, Some(("mcp.example.test".into(), 8443)));
}

#[test]
fn should_reject_credentials_in_allowed_host_authority() {
    // Arrange
    let authority = "attacker@mcp.example.test";

    // Act
    let result = parse_authority(authority);

    // Assert
    assert!(result.is_none());
}

#[test]
fn should_normalize_https_origins_and_default_ports() {
    // Arrange
    let origin_without_port = parse_origin("https://console.example.test").unwrap();
    let origin_with_default_port = parse_origin("https://console.example.test:443").unwrap();

    // Act
    let origin_with_other_port = parse_origin("https://console.example.test:8443").unwrap();

    // Assert
    assert_eq!(origin_without_port, origin_with_default_port);
    assert_ne!(origin_without_port, origin_with_other_port);
    assert_eq!(
        origin_without_port.config_entry(),
        "https://console.example.test:443"
    );
}

#[test]
fn should_reject_non_origin_urls_from_allowlist() {
    // Arrange
    let http_origin = "http://console.example.test";
    let path_origin = "https://console.example.test/admin";
    let query_origin = "https://console.example.test?next=/admin";

    // Act
    let results = [http_origin, path_origin, query_origin].map(parse_origin);

    // Assert
    assert!(results.iter().all(Result::is_err));
}

#[tokio::test]
async fn should_serve_primary_http_discovery_and_scoped_diagnostic_prompt() {
    // Arrange
    let (url, stop, server) = spawn_authenticated_mcp_service().await;
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(url),
    );
    let client = ClientConfig::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .expect("primary stateless discovery should succeed");

    // Act
    let tools = client.list_tools(None).await.expect("tool listing");
    let arguments = serde_json::from_value(serde_json::json!({
        "route_family": "41",
        "realm": "customer-space",
        "area": "jobs",
        "resource": "dispatch"
    }))
    .expect("prompt arguments");
    let prompt = client
        .get_prompt(GetPromptRequestParams::new("diagnose_kv").with_arguments(arguments))
        .await
        .expect("scoped diagnostic prompt");
    let prompt_text = format!("{prompt:?}");

    // Assert
    assert_eq!(tools.tools.len(), 9);
    assert!(prompt_text.contains("Resource detail"));
    assert!(prompt_text.contains("Recent timeline"));
    client.cancel().await.expect("close primary client");
    stop.cancel();
    server.await.expect("HTTP listener task");
}

#[tokio::test]
async fn should_serve_compatibility_http_initialize_and_tool_call() {
    // Arrange
    let (url, stop, server) = spawn_authenticated_mcp_service().await;
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(url),
    );
    let client = ClientConfig::new(
        rmcp::model::ClientCapabilities::default(),
        Implementation::new("fitz-compatibility-test", "0.0.1"),
    )
    .with_protocol_version(crate::api::mcp::catalog::compatibility_revision())
    .serve_with_lifecycle(transport, ClientLifecycleMode::Initialize)
    .await
    .expect("compatibility initialize should succeed");

    // Act
    let result = client
        .call_tool(CallToolRequestParams::new("get_global_stats"))
        .await
        .expect("global stats call");

    // Assert
    assert_ne!(result.is_error, Some(true));
    client.cancel().await.expect("close compatibility client");
    stop.cancel();
    server.await.expect("HTTP listener task");
}

async fn spawn_authenticated_mcp_service(
) -> (String, CancellationToken, tokio::task::JoinHandle<()>) {
    let runtime = Arc::new(Runtime::with_admin_read_model(
        Arc::new(Router::new()),
        AdminReadModel::new(),
    ));
    let authenticated = AuthenticatedRequest {
        context: crate::api::mcp::McpExecutionContext::authenticated(
            AdminPrincipal {
                username: "protocol-test".into(),
                route_family_access: AdminRouteFamilyAccess::wildcard(),
            },
            SessionPermissions::all(),
        ),
        policy: crate::api::mcp::McpCapabilityPolicy::read_only(),
        token_fingerprint: [7; 32],
        expires_at: SystemTime::now() + Duration::from_secs(300),
    };
    let stop = CancellationToken::new();
    let inner = build_inner_service(
        runtime,
        None,
        vec!["127.0.0.1".into()],
        vec!["https://console.example.test:443".into()],
        stop.child_token(),
        Arc::new(Semaphore::new(server::MAX_EXECUTIONS)),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral MCP listener");
    let address = listener.local_addr().expect("listener address");
    let server_stop = stop.clone();
    let server = tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                () = server_stop.cancelled() => break,
                accepted = listener.accept() => accepted,
            };
            let Ok((stream, _)) = accepted else {
                continue;
            };
            let inner = inner.clone();
            let authenticated = authenticated.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    let inner = inner.clone();
                    let authenticated = authenticated.clone();
                    async move {
                        let response = CURRENT_AUTHENTICATED_REQUEST
                            .scope(authenticated, inner.handle(request))
                            .await;
                        Ok::<_, Infallible>(response)
                    }
                });
                let _ = http1::Builder::new()
                    .keep_alive(true)
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    (format!("http://{address}/mcp"), stop, server)
}

#[tokio::test]
async fn should_reject_unconfigured_host_origin_and_oversized_declared_body() {
    // Arrange
    let state = http_state_for_tests();
    let bad_host = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(HOST, "evil.example.test")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let bad_origin = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(HOST, "fitz.example.test")
        .header(ORIGIN, "https://evil.example.test")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let oversized = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(HOST, "fitz.example.test")
        .header(CONTENT_LENGTH, (MAX_REQUEST_BODY_BYTES + 1).to_string())
        .body(Full::new(Bytes::new()))
        .unwrap();

    // Act
    let host_response = state.handle(bad_host).await;
    let origin_response = state.handle(bad_origin).await;
    let body_response = state.handle(oversized).await;

    // Assert
    assert_eq!(host_response.status(), StatusCode::FORBIDDEN);
    assert_eq!(origin_response.status(), StatusCode::FORBIDDEN);
    assert_eq!(body_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

fn http_state_for_tests() -> McpHttpState {
    let runtime = Arc::new(Runtime::with_admin_read_model(
        Arc::new(Router::new()),
        AdminReadModel::new(),
    ));
    let verifier = oauth::McpOAuthVerifier::new(
        "https://identity.example.test".into(),
        "fitz-tests".into(),
        "https://fitz.example.test/mcp",
        include_bytes!("testdata/insecure_test_only_rsa_public.pem"),
        None,
    )
    .expect("test verifier");
    let allowed_origins = vec![ValidatedOrigin {
        host: "console.example.test".into(),
        port: 443,
    }];
    let inner = build_inner_service(
        runtime.clone(),
        None,
        vec!["fitz.example.test".into()],
        vec!["https://console.example.test:443".into()],
        CancellationToken::new(),
        Arc::new(Semaphore::new(server::MAX_EXECUTIONS)),
    );
    McpHttpState {
        verifier: Arc::new(verifier),
        inner,
        runtime,
        request_slots: Arc::new(Semaphore::new(MAX_HTTP_CONCURRENCY)),
        sessions: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        audit_buffer: McpAuditBuffer::new(),
        allowed_host: "fitz.example.test".into(),
        allowed_port: 443,
        allowed_origins,
    }
}
