use crate::api::mcp::catalog::{compatibility_revision, primary_revision};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, ClientConfig, GetPromptRequestParams,
    GetPromptResponse, Implementation, ListPromptsResult, ListResourceTemplatesResult,
    ListResourcesResult, ListToolsResult, PaginatedRequestParams, ProtocolVersion,
    ReadResourceRequestParams, ReadResourceResponse, ServerCapabilities, ServerConfig,
};
use rmcp::service::{
    ClientLifecycleMode, ClientServiceExt, Peer, RoleClient, RoleServer, ServiceExt,
};
use rmcp::transport::stdio;
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use rmcp::{ErrorData, ServerHandler};
use std::borrow::Cow;
use std::env;
use std::fs;
use std::path::Path;
use url::Url;

const MAX_BEARER_TOKEN_BYTES: usize = 16 * 1024;

/// Run the authenticated HTTP-to-stdio MCP proxy until the local client closes.
///
/// # Errors
/// Returns runtime startup or upstream protocol failures.
pub fn run_stdio_adapter() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::var("FITZ_MCP_HTTP_URL")?;
    validate_endpoint(&endpoint)?;
    let token_path = env::var("FITZ_MCP_BEARER_TOKEN_FILE")?;
    let token = read_bearer_token(Path::new(&token_path))?;
    let config = StreamableHttpClientTransportConfig::with_uri(endpoint)
        .auth_header(token)
        .max_concurrent_requests(8);
    let transport = StreamableHttpClientTransport::with_client(reqwest::Client::new(), config);
    let mut remote = ClientConfig::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Auto {
                preferred_versions: vec![primary_revision(), compatibility_revision()],
                legacy_version: Some(compatibility_revision()),
            },
        )
        .await?;
    let proxy = StdioProxy {
        remote: remote.peer().clone(),
    };
    let local = proxy.serve(stdio()).await?;
    local.waiting().await?;
    remote.close().await?;
    Ok(())
}

fn validate_endpoint(value: &str) -> Result<(), &'static str> {
    let endpoint = Url::parse(value).map_err(|_| "FITZ_MCP_HTTP_URL must be an absolute URL")?;
    let local_http = endpoint.scheme() == "http"
        && endpoint.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
    if (endpoint.scheme() != "https" && !local_http)
        || endpoint.host_str().is_none()
        || endpoint.username() != ""
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || endpoint.path() != "/mcp"
    {
        return Err("FITZ_MCP_HTTP_URL must be an HTTPS /mcp URL (HTTP is allowed for loopback development)");
    }
    Ok(())
}

fn read_bearer_token(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    if !path.is_absolute() {
        return Err("FITZ_MCP_BEARER_TOKEN_FILE must be an absolute path".into());
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("FITZ_MCP_BEARER_TOKEN_FILE must name a regular non-symlink file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("bearer token file permissions must exclude group and other users".into());
        }
    }
    let token = fs::read_to_string(path)?;
    let token = token.trim();
    if token.is_empty()
        || token.len() > MAX_BEARER_TOKEN_BYTES
        || token.chars().any(char::is_whitespace)
    {
        return Err("bearer token file must contain one bounded token".into());
    }
    Ok(token.to_string())
}

#[derive(Clone)]
struct StdioProxy {
    remote: Peer<RoleClient>,
}

impl ServerHandler for StdioProxy {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
        )
        .with_protocol_version(primary_revision())
        .with_server_info(Implementation::new("fitz-mcp-stdio", env!("CARGO_PKG_VERSION")))
        .with_instructions("This stdio adapter forwards requests to the configured authenticated Fitz MCP endpoint.")
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Owned(vec![primary_revision(), compatibility_revision()])
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        self.remote
            .list_tools(request)
            .await
            .map_err(upstream_error)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        self.remote
            .call_tool_once(request)
            .await
            .map_err(upstream_error)
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        self.remote
            .list_resources(request)
            .await
            .map_err(upstream_error)
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        self.remote
            .list_resource_templates(request)
            .await
            .map_err(upstream_error)
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        self.remote
            .read_resource_once(request)
            .await
            .map_err(upstream_error)
    }

    async fn list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        self.remote
            .list_prompts(request)
            .await
            .map_err(upstream_error)
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, ErrorData> {
        self.remote
            .get_prompt_once(request)
            .await
            .map_err(upstream_error)
    }
}

fn upstream_error(_error: impl std::fmt::Display) -> ErrorData {
    ErrorData::internal_error("Fitz MCP upstream request failed", None)
}
