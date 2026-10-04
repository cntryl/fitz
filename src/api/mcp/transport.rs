mod actions;
mod oauth;
mod server;

use self::actions::McpActionState;
use self::oauth::{AuthError, AuthenticatedRequest, McpOAuthEnvironment, McpOAuthVerifier};
use self::server::McpServer;
use crate::api::mcp::McpAuditBuffer;
use crate::boot::Runtime;
use http_body_util::{combinators::BoxBody, BodyExt, Full, Limited};
use hyper::header::{AUTHORIZATION, CONTENT_LENGTH, HOST, ORIGIN, WWW_AUTHENTICATE};
use hyper::{Method, Request, Response, StatusCode};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
const MAX_HTTP_CONCURRENCY: usize = 32;
const MAX_SESSION_BINDINGS: usize = 4_096;
const SESSION_IDLE_TTL: Duration = Duration::from_hours(1);

type InnerService = StreamableHttpService<McpServer, LocalSessionManager>;
pub(crate) type McpResponse = Response<BoxBody<bytes::Bytes, Infallible>>;

tokio::task_local! {
    static CURRENT_AUTHENTICATED_REQUEST: AuthenticatedRequest;
}

#[derive(Clone)]
pub(crate) struct McpHttpState {
    verifier: Arc<McpOAuthVerifier>,
    inner: InnerService,
    runtime: Arc<Runtime>,
    request_slots: Arc<Semaphore>,
    sessions: Arc<parking_lot::Mutex<HashMap<String, SessionBinding>>>,
    audit_buffer: McpAuditBuffer,
    allowed_host: String,
    allowed_port: u16,
    allowed_origins: Vec<ValidatedOrigin>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ValidatedOrigin {
    host: String,
    port: u16,
}

impl ValidatedOrigin {
    fn config_entry(&self) -> String {
        format!("https://{}:{}", format_url_host(&self.host), self.port)
    }
}

#[derive(Clone)]
struct SessionBinding {
    token_fingerprint: [u8; 32],
    expires_at: SystemTime,
    last_seen: Instant,
}

impl McpHttpState {
    pub(crate) fn from_env(
        runtime: Arc<Runtime>,
        cancellation_token: CancellationToken,
    ) -> Result<Option<Self>, String> {
        match std::env::var("FITZ_MCP_HTTP_ENABLED")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("false" | "0") => return Ok(None),
            Some("true" | "1") => {}
            Some(_) => return Err("FITZ_MCP_HTTP_ENABLED must be true or false".into()),
        }

        let environment = McpOAuthEnvironment::from_env()?;
        let public_key = std::fs::read(&environment.public_key_file)
            .map_err(|error| format!("could not read FITZ_MCP_OAUTH_PUBLIC_KEY_FILE: {error}"))?;
        let verifier = McpOAuthVerifier::new(
            environment.issuer,
            environment.audience,
            &environment.public_url,
            &public_key,
            environment.documentation_url,
        )?;
        let public_url =
            url::Url::parse(&environment.public_url).map_err(|_| "invalid MCP public URL")?;
        let host = public_url
            .host_str()
            .ok_or("MCP public URL must have a host")?
            .to_ascii_lowercase();
        let port = public_url
            .port_or_known_default()
            .ok_or("MCP public URL must use a known port")?;
        let default_origin = ValidatedOrigin {
            host: host.clone(),
            port,
        };
        let allowed_origins = match environment.allowed_origins.as_deref() {
            Some(raw) => raw
                .split(',')
                .map(str::trim)
                .map(parse_origin)
                .collect::<Result<Vec<_>, _>>()?,
            None => vec![default_origin],
        };
        if allowed_origins.is_empty() {
            return Err("FITZ_MCP_ALLOWED_ORIGINS must contain at least one HTTPS origin".into());
        }
        let allowed_origin_entries = allowed_origins
            .iter()
            .map(ValidatedOrigin::config_entry)
            .collect();
        let execution_slots = Arc::new(Semaphore::new(server::MAX_EXECUTIONS));
        let actions = McpActionState::from_env()?.map(Arc::new);
        let inner = build_inner_service(
            runtime.clone(),
            actions.clone(),
            vec![host.clone()],
            allowed_origin_entries,
            cancellation_token,
            execution_slots,
        );
        Ok(Some(Self {
            verifier: Arc::new(verifier),
            inner,
            runtime,
            request_slots: Arc::new(Semaphore::new(MAX_HTTP_CONCURRENCY)),
            sessions: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            audit_buffer: McpAuditBuffer::new(),
            allowed_host: host,
            allowed_port: port,
            allowed_origins,
        }))
    }

    #[allow(clippy::too_many_lines)] // This wrapper enforces the ordered HTTP boundary checks.
    pub(crate) async fn handle<B>(&self, request: Request<B>) -> McpResponse
    where
        B: hyper::body::Body + Send + 'static,
        B::Data: bytes::Buf + Send + 'static,
        B::Error: Display + Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let is_metadata = request.method() == Method::GET
            && request.uri().path() == "/.well-known/oauth-protected-resource/mcp";
        if !is_metadata && request.uri().path() != "/mcp" {
            return json_error(StatusCode::NOT_FOUND, "not_found", None);
        }
        if !self.authority_is_allowed(&request) {
            crate::api::mcp::telemetry::record_denial();
            return json_error(StatusCode::FORBIDDEN, "invalid_host", None);
        }
        if !self.origin_is_allowed(&request) {
            crate::api::mcp::telemetry::record_denial();
            return json_error(StatusCode::FORBIDDEN, "invalid_origin", None);
        }
        if is_metadata {
            return json_response(StatusCode::OK, &self.verifier.metadata());
        }

        let Ok(_request_permit) = self.request_slots.clone().try_acquire_owned() else {
            crate::api::mcp::telemetry::record_overload();
            return json_error(StatusCode::TOO_MANY_REQUESTS, "server_busy", Some("1"));
        };
        let declared_length = match request.headers().get(CONTENT_LENGTH) {
            Some(value) => match value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
            {
                Some(length) => Some(length),
                None => return json_error(StatusCode::BAD_REQUEST, "invalid_content_length", None),
            },
            None => None,
        };
        if declared_length.is_some_and(|length| length > MAX_REQUEST_BODY_BYTES) {
            return json_error(StatusCode::PAYLOAD_TOO_LARGE, "request_too_large", None);
        }

        let authenticated = match self.verifier.authenticate(
            request.headers().get(AUTHORIZATION),
            self.runtime.as_ref(),
            self.audit_buffer.clone(),
        ) {
            Ok(authenticated) => authenticated,
            Err(error) => return self.auth_error_response(&error),
        };
        let session_id = request
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if let Some(session_id) = session_id.as_deref() {
            if let Err(status) = self.authorize_session(session_id, &authenticated) {
                crate::api::mcp::telemetry::record_denial();
                return json_error(status, "session_authority_mismatch", None);
            }
        }

        let (parts, body) = request.into_parts();
        let Ok(collected) = Limited::new(body, MAX_REQUEST_BODY_BYTES).collect().await else {
            return json_error(StatusCode::PAYLOAD_TOO_LARGE, "request_too_large", None);
        };
        let body_bytes = collected.to_bytes();
        if declared_length.is_some_and(|length| length != body_bytes.len()) {
            return json_error(StatusCode::BAD_REQUEST, "content_length_mismatch", None);
        }
        let request = Request::from_parts(parts, Full::new(body_bytes));

        let fingerprint = authenticated.token_fingerprint;
        let is_delete = request.method() == Method::DELETE;
        let response = CURRENT_AUTHENTICATED_REQUEST
            .scope(authenticated.clone(), self.inner.clone().handle(request))
            .await;
        if is_delete {
            if let Some(session_id) = session_id {
                self.sessions.lock().remove(&session_id);
            }
        }
        if let Some(session_id) = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
        {
            if !self.bind_session(session_id.clone(), fingerprint, authenticated.expires_at) {
                let cleanup = Request::builder()
                    .method(Method::DELETE)
                    .uri("/mcp")
                    .header("mcp-session-id", session_id)
                    .body(Full::new(bytes::Bytes::new()))
                    .expect("static cleanup request is valid");
                let _ = self.inner.clone().handle(cleanup).await;
                return json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "session_capacity_full",
                    Some("1"),
                );
            }
        }
        response
    }

    fn authorize_session(
        &self,
        session_id: &str,
        authenticated: &AuthenticatedRequest,
    ) -> Result<(), StatusCode> {
        let now = Instant::now();
        let mut sessions = self.sessions.lock();
        sessions.retain(|_, binding| {
            binding.expires_at > SystemTime::now()
                && now.duration_since(binding.last_seen) < SESSION_IDLE_TTL
        });
        let binding = sessions.get_mut(session_id).ok_or(StatusCode::NOT_FOUND)?;
        if binding.token_fingerprint != authenticated.token_fingerprint {
            return Err(StatusCode::UNAUTHORIZED);
        }
        binding.last_seen = now;
        Ok(())
    }

    fn bind_session(
        &self,
        session_id: String,
        token_fingerprint: [u8; 32],
        expires_at: SystemTime,
    ) -> bool {
        let now = Instant::now();
        let mut sessions = self.sessions.lock();
        sessions.retain(|_, binding| {
            binding.expires_at > SystemTime::now()
                && now.duration_since(binding.last_seen) < SESSION_IDLE_TTL
        });
        if !sessions.contains_key(&session_id) && sessions.len() >= MAX_SESSION_BINDINGS {
            return false;
        }
        sessions.insert(
            session_id,
            SessionBinding {
                token_fingerprint,
                expires_at,
                last_seen: now,
            },
        );
        true
    }

    fn auth_error_response(&self, error: &AuthError) -> McpResponse {
        crate::api::mcp::telemetry::record_authentication_failure();
        let challenge = format!(
            "Bearer resource_metadata=\"{}\", error=\"{}\"",
            self.verifier.metadata_url(),
            error.code()
        );
        let mut response = json_error(error.status_code(), error.code(), None);
        if let Ok(value) = hyper::header::HeaderValue::from_str(&challenge) {
            response.headers_mut().insert(WWW_AUTHENTICATE, value);
        }
        response
    }

    fn authority_is_allowed<B>(&self, request: &Request<B>) -> bool {
        let uri_authority = request
            .uri()
            .authority()
            .map(hyper::http::uri::Authority::as_str);
        let header_authority = request
            .headers()
            .get(HOST)
            .and_then(|value| value.to_str().ok());
        if uri_authority.is_some()
            && header_authority.is_some()
            && parse_authority(uri_authority.unwrap()) != parse_authority(header_authority.unwrap())
        {
            return false;
        }
        let Some(authority) = uri_authority.or(header_authority) else {
            return false;
        };
        parse_authority(authority)
            .is_some_and(|(host, port)| host == self.allowed_host && port == self.allowed_port)
    }

    fn origin_is_allowed<B>(&self, request: &Request<B>) -> bool {
        let Some(value) = request.headers().get(ORIGIN) else {
            return true;
        };
        value
            .to_str()
            .ok()
            .and_then(|value| parse_origin(value).ok())
            .is_some_and(|origin| self.allowed_origins.contains(&origin))
    }
}

fn build_inner_service(
    runtime: Arc<Runtime>,
    actions: Option<Arc<McpActionState>>,
    allowed_hosts: Vec<String>,
    allowed_origins: Vec<String>,
    cancellation_token: CancellationToken,
    execution_slots: Arc<Semaphore>,
) -> InnerService {
    let mut config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(true)
        .with_json_response(true)
        .with_max_request_body_bytes(MAX_REQUEST_BODY_BYTES)
        .with_cancellation_token(cancellation_token);
    config.allowed_hosts = allowed_hosts;
    config.allowed_origins = allowed_origins;
    config = config.enforce_origin_validation();
    StreamableHttpService::new(
        move || {
            let authenticated = CURRENT_AUTHENTICATED_REQUEST
                .try_with(Clone::clone)
                .map_err(|_| std::io::Error::other("MCP request has no authenticated authority"))?;
            Ok(McpServer::from_authenticated_request(
                runtime.clone(),
                authenticated,
                actions.clone(),
                execution_slots.clone(),
            ))
        },
        Arc::new(LocalSessionManager::default()),
        config,
    )
}

fn parse_authority(value: &str) -> Option<(String, u16)> {
    if value.contains('@') {
        return None;
    }
    let authority = hyper::http::uri::Authority::try_from(value).ok()?;
    let parsed = url::Url::parse(&format!("https://{authority}/")).ok()?;
    if parsed.username() != "" || parsed.password().is_some() {
        return None;
    }
    Some((
        parsed.host_str()?.to_ascii_lowercase(),
        parsed.port_or_known_default()?,
    ))
}

fn parse_origin(value: &str) -> Result<ValidatedOrigin, String> {
    let parsed = url::Url::parse(value).map_err(|_| "invalid MCP allowed origin")?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(
            "MCP allowed origins must be HTTPS origins without credentials, paths or queries"
                .into(),
        );
    }
    Ok(ValidatedOrigin {
        host: parsed.host_str().unwrap().to_ascii_lowercase(),
        port: parsed
            .port_or_known_default()
            .ok_or("MCP allowed origin must use a known port")?,
    })
}

fn format_url_host(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

fn json_response(status: StatusCode, value: &serde_json::Value) -> McpResponse {
    let body = serde_json::to_vec(value).unwrap_or_default();
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(bytes::Bytes::from(body)).boxed())
        .expect("valid MCP JSON response")
}

fn json_error(status: StatusCode, code: &str, retry_after: Option<&str>) -> McpResponse {
    let mut builder = Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json");
    if let Some(retry_after) = retry_after {
        builder = builder.header(hyper::header::RETRY_AFTER, retry_after);
    }
    builder
        .body(Full::new(bytes::Bytes::from(format!(r#"{{"error":"{code}"}}"#))).boxed())
        .expect("valid MCP error response")
}

#[cfg(test)]
mod tests;
