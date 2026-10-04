use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ClientConfig, Implementation,
    ListToolsResult, ProtocolVersion, ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::{ClientLifecycleMode, ClientServiceExt};
use rmcp::transport::child_process::TokioChildProcess;
use rmcp::{ErrorData, RoleServer, ServerHandler};
use std::borrow::Cow;
use std::convert::Infallible;
use std::process::Stdio;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct EchoServer;

impl ServerHandler for EchoServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::V_2026_07_28)
            .with_server_info(Implementation::new("stdio-upstream-test", "0.0.1"))
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Owned(vec![
            ProtocolVersion::V_2026_07_28,
            ProtocolVersion::V_2025_11_25,
        ])
    }

    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(vec![Tool::new(
            "echo",
            "Return the supplied text.",
            serde_json::Map::new(),
        )]))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let text = request
            .arguments
            .as_ref()
            .and_then(|arguments| arguments.get("text"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        Ok(CallToolResult::structured(serde_json::json!({ "echo": text })).into())
    }
}

#[tokio::test]
async fn should_forward_real_stdio_client_requests_to_remote_http_service() {
    // Arrange
    let (url, stop, server) = spawn_upstream().await;
    let token_file = tempfile::NamedTempFile::new().expect("temporary token file");
    std::fs::write(token_file.path(), "test-only-bearer-token\n").expect("write token");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(token_file.path(), std::fs::Permissions::from_mode(0o600))
            .expect("restrict token file");
    }
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_fitz-mcp-stdio"));
    command
        .env("FITZ_MCP_HTTP_URL", url)
        .env("FITZ_MCP_BEARER_TOKEN_FILE", token_file.path())
        .stderr(Stdio::null());
    let transport = TokioChildProcess::new(command).expect("spawn stdio adapter");
    let client = ClientConfig::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .expect("stdio MCP client should connect");

    // Act
    let tools = client.list_tools(None).await.expect("forwarded tool list");
    let arguments = serde_json::from_value(serde_json::json!({ "text": "through-stdio" }))
        .expect("tool arguments");
    let result = client
        .call_tool(CallToolRequestParams::new("echo").with_arguments(arguments))
        .await
        .expect("forwarded tool call");

    // Assert
    assert_eq!(tools.tools.len(), 1);
    assert_ne!(result.is_error, Some(true));
    assert!(format!("{result:?}").contains("through-stdio"));
    client.cancel().await.expect("close local MCP client");
    stop.cancel();
    server.await.expect("upstream HTTP listener");
}

async fn spawn_upstream() -> (String, CancellationToken, tokio::task::JoinHandle<()>) {
    let stop = CancellationToken::new();
    let config = rmcp::transport::streamable_http_server::StreamableHttpServerConfig::default()
        .with_legacy_session_mode(true)
        .with_json_response(true)
        .with_cancellation_token(stop.child_token());
    let service = rmcp::transport::streamable_http_server::StreamableHttpService::new(
        || Ok(EchoServer),
        std::sync::Arc::new(
            rmcp::transport::streamable_http_server::session::local::LocalSessionManager::default(),
        ),
        config,
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream listener");
    let address = listener.local_addr().expect("upstream address");
    let server_stop = stop.clone();
    let server = tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                _ = server_stop.cancelled() => break,
                accepted = listener.accept() => accepted,
            };
            let Ok((stream, _)) = accepted else {
                continue;
            };
            let service = service.clone();
            tokio::spawn(async move {
                let connection = service_fn(move |request| {
                    let service = service.clone();
                    async move { Ok::<_, Infallible>(service.handle(request).await) }
                });
                let _ = http1::Builder::new()
                    .keep_alive(true)
                    .serve_connection(TokioIo::new(stream), connection)
                    .await;
            });
        }
    });
    (format!("http://{address}/mcp"), stop, server)
}
