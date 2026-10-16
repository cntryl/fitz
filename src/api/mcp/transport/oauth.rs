use crate::api::admin::auth::{AdminPrincipal, AdminRouteFamilyAccess};
use crate::api::mcp::McpAuditBuffer;
use crate::api::mcp::{McpCapabilityClass, McpCapabilityPolicy, McpExecutionContext};
use crate::auth::Permission;
use crate::boot::Runtime;
use crate::session::permissions::SessionPermissions;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

const MAX_BEARER_TOKEN_BYTES: usize = 16 * 1024;
const MAX_SUBJECT_BYTES: usize = 256;
const MAX_PERMISSION_COUNT: usize = 128;
const MAX_PERMISSION_BYTES: usize = 512;
const MAX_ROUTE_FAMILY_COUNT: usize = 256;
const MAX_CAPABILITY_COUNT: usize = 8;
const SUPPORTED_SCOPES: [&str; 3] = ["fitz.mcp.read", "fitz.mcp.mutate", "fitz.mcp.admin"];

#[derive(Clone)]
pub(super) struct McpOAuthVerifier {
    issuer: String,
    audience: String,
    resource_url: String,
    metadata_url: String,
    documentation_url: Option<String>,
    key: DecodingKey,
}

#[derive(Debug, Clone)]
pub(super) struct AuthenticatedRequest {
    pub context: McpExecutionContext,
    pub policy: McpCapabilityPolicy,
    pub token_fingerprint: [u8; 32],
    pub expires_at: SystemTime,
}

#[derive(Debug, Deserialize)]
struct McpOAuthClaims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
    #[serde(default)]
    nbf: Option<u64>,
    fitz_role: String,
    fitz_route_families: AdminRouteFamilyAccess,
    fitz_permissions: Vec<String>,
    fitz_mcp_capabilities: Vec<McpCapabilityClass>,
    scope: ScopeClaim,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ScopeClaim {
    String(String),
    Array(Vec<String>),
}

impl ScopeClaim {
    fn values(&self) -> impl Iterator<Item = &str> {
        let values: Vec<&str> = match self {
            Self::String(value) => value.split_ascii_whitespace().collect(),
            Self::Array(values) => values.iter().map(String::as_str).collect(),
        };
        values.into_iter()
    }
}

impl McpOAuthVerifier {
    pub(super) fn new(
        issuer: String,
        audience: String,
        public_url: &str,
        public_key_pem: &[u8],
        documentation_url: Option<String>,
    ) -> Result<Self, String> {
        let issuer_url = Url::parse(&issuer).map_err(|_| "invalid MCP OAuth issuer URL")?;
        if issuer_url.scheme() != "https"
            || issuer_url.host_str().is_none()
            || issuer_url.username() != ""
            || issuer_url.password().is_some()
            || issuer_url.query().is_some()
            || issuer_url.fragment().is_some()
        {
            return Err(
                "MCP OAuth issuer must be an HTTPS origin without credentials or query".into(),
            );
        }
        let resource = Url::parse(public_url).map_err(|_| "invalid MCP public URL")?;
        if resource.scheme() != "https"
            || resource.host_str().is_none()
            || resource.username() != ""
            || resource.password().is_some()
            || resource.query().is_some()
            || resource.fragment().is_some()
            || resource.path() != "/mcp"
        {
            return Err("MCP public URL must be an HTTPS URL ending in /mcp".into());
        }
        if audience.trim().is_empty() {
            return Err("MCP OAuth audience must not be empty".into());
        }
        if let Some(documentation_url) = documentation_url.as_deref() {
            let documentation =
                Url::parse(documentation_url).map_err(|_| "invalid MCP documentation URL")?;
            if documentation.scheme() != "https" || documentation.host_str().is_none() {
                return Err("MCP documentation URL must use HTTPS".into());
            }
        }
        let key = DecodingKey::from_rsa_pem(public_key_pem)
            .map_err(|_| "MCP OAuth public key is not a valid RSA PEM key")?;
        let metadata_url = protected_resource_metadata_url(&resource);
        Ok(Self {
            issuer,
            audience,
            resource_url: resource.to_string(),
            metadata_url,
            documentation_url,
            key,
        })
    }

    pub(super) fn metadata_url(&self) -> &str {
        &self.metadata_url
    }

    pub(super) fn metadata(&self) -> serde_json::Value {
        let mut metadata = serde_json::json!({
            "resource": self.resource_url,
            "authorization_servers": [self.issuer],
            "scopes_supported": SUPPORTED_SCOPES,
            "bearer_methods_supported": ["header"]
        });
        if let Some(documentation_url) = &self.documentation_url {
            metadata["resource_documentation"] = documentation_url.clone().into();
        }
        metadata
    }

    pub(super) fn authenticate(
        &self,
        authorization: Option<&hyper::header::HeaderValue>,
        runtime: &Runtime,
        audit_buffer: McpAuditBuffer,
    ) -> Result<AuthenticatedRequest, AuthError> {
        let token = authorization
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .filter(|token| !token.is_empty() && token.len() <= MAX_BEARER_TOKEN_BYTES)
            .ok_or(AuthError::MissingOrMalformed)?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        validation.leeway = 0;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.required_spec_claims = ["aud", "exp", "iss", "sub"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let claims = decode::<McpOAuthClaims>(token, &self.key, &validation)
            .map_err(|_| AuthError::InvalidToken)?
            .claims;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuthError::InvalidToken)?
            .as_secs();
        if claims.iss != self.issuer
            || claims.sub.is_empty()
            || claims.sub.len() > MAX_SUBJECT_BYTES
            || claims.sub.chars().any(char::is_control)
            || claims.iat > now
            || claims.exp <= now
            || claims.nbf.is_some_and(|not_before| not_before > now)
            || claims.fitz_role != "admin"
        {
            return Err(AuthError::InvalidToken);
        }

        validate_route_family_access(&claims.fitz_route_families, runtime)?;
        let permissions = parse_permissions(&claims.fitz_permissions)?;
        let scopes: BTreeSet<&str> = claims.scope.values().collect();
        let policy = provision_capabilities(&claims.fitz_mcp_capabilities, &scopes)?;
        let principal = AdminPrincipal {
            username: claims.sub,
            route_family_access: claims.fitz_route_families,
        };
        let token_fingerprint: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let expires_at = UNIX_EPOCH + std::time::Duration::from_secs(claims.exp);
        Ok(AuthenticatedRequest {
            context: McpExecutionContext::authenticated_with_audit(
                principal,
                SessionPermissions::from_permissions(permissions),
                audit_buffer,
            ),
            policy,
            token_fingerprint,
            expires_at,
        })
    }
}

#[derive(Debug)]
pub(super) enum AuthError {
    MissingOrMalformed,
    InvalidToken,
    InvalidProvisioning,
}

impl AuthError {
    pub(super) fn status_code(&self) -> hyper::StatusCode {
        match self {
            Self::MissingOrMalformed | Self::InvalidToken => hyper::StatusCode::UNAUTHORIZED,
            Self::InvalidProvisioning => hyper::StatusCode::FORBIDDEN,
        }
    }

    pub(super) fn code(&self) -> &'static str {
        match self {
            Self::MissingOrMalformed => "invalid_request",
            Self::InvalidToken => "invalid_token",
            Self::InvalidProvisioning => "insufficient_scope",
        }
    }
}

pub(super) struct McpOAuthEnvironment {
    pub issuer: String,
    pub audience: String,
    pub public_url: String,
    pub public_key_file: PathBuf,
    pub documentation_url: Option<String>,
    pub allowed_origins: Option<String>,
}

impl McpOAuthEnvironment {
    pub(super) fn from_env() -> Result<Self, String> {
        let issuer = required_env("FITZ_MCP_OAUTH_ISSUER")?;
        let audience = required_env("FITZ_MCP_OAUTH_AUDIENCE")?;
        let public_url = required_env("FITZ_MCP_PUBLIC_URL")?;
        let public_key_file = PathBuf::from(required_env("FITZ_MCP_OAUTH_PUBLIC_KEY_FILE")?);
        let documentation_url = optional_env("FITZ_MCP_DOCUMENTATION_URL");
        let allowed_origins = optional_env("FITZ_MCP_ALLOWED_ORIGINS");
        Ok(Self {
            issuer,
            audience,
            public_url,
            public_key_file,
            documentation_url,
            allowed_origins,
        })
    }
}

pub(super) fn required_env(name: &str) -> Result<String, String> {
    optional_env(name).ok_or_else(|| format!("{name} is required when MCP HTTP is enabled"))
}

fn optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn protected_resource_metadata_url(resource: &Url) -> String {
    let mut metadata = resource.clone();
    metadata.set_path("/.well-known/oauth-protected-resource/mcp");
    metadata.to_string()
}

fn validate_route_family_access(
    access: &AdminRouteFamilyAccess,
    runtime: &Runtime,
) -> Result<(), AuthError> {
    match access {
        AdminRouteFamilyAccess::Wildcard(value) if value == "*" => Ok(()),
        AdminRouteFamilyAccess::Explicit(values)
            if !values.is_empty()
                && values.len() <= MAX_ROUTE_FAMILY_COUNT
                && values.iter().all(|value| {
                    value.parse::<u32>().is_ok_and(|family| {
                        family.to_string() == *value
                            && runtime.admin_auth().is_provisioned_route_family(family)
                    })
                }) =>
        {
            Ok(())
        }
        _ => Err(AuthError::InvalidProvisioning),
    }
}

#[cfg(test)]
mod tests;

fn parse_permissions(raw: &[String]) -> Result<Vec<Permission>, AuthError> {
    if raw.is_empty() || raw.len() > MAX_PERMISSION_COUNT {
        return Err(AuthError::InvalidProvisioning);
    }
    raw.iter()
        .map(|value| {
            if value.len() > MAX_PERMISSION_BYTES {
                return Err(AuthError::InvalidProvisioning);
            }
            Permission::parse(value).map_err(|_| AuthError::InvalidProvisioning)
        })
        .collect()
}

fn provision_capabilities(
    raw: &[McpCapabilityClass],
    scopes: &BTreeSet<&str>,
) -> Result<McpCapabilityPolicy, AuthError> {
    if raw.is_empty() || raw.len() > MAX_CAPABILITY_COUNT {
        return Err(AuthError::InvalidProvisioning);
    }
    let classes: BTreeSet<_> = raw.iter().copied().collect();
    if classes.contains(&McpCapabilityClass::Summary)
        || classes.contains(&McpCapabilityClass::Inspect)
        || classes.contains(&McpCapabilityClass::Explain)
    {
        require_scope(scopes, "fitz.mcp.read")?;
    }
    if classes.contains(&McpCapabilityClass::Mutate) {
        require_scope(scopes, "fitz.mcp.mutate")?;
    }
    if classes.contains(&McpCapabilityClass::Admin) {
        require_scope(scopes, "fitz.mcp.admin")?;
    }
    Ok(McpCapabilityPolicy::from_classes(classes))
}

fn require_scope(scopes: &BTreeSet<&str>, required: &str) -> Result<(), AuthError> {
    scopes
        .contains(required)
        .then_some(())
        .ok_or(AuthError::InvalidProvisioning)
}
