use argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
};
use chrono::{Duration, Utc};
use hyper::StatusCode;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

mod config;
mod cookies;
mod origin_checks;
mod responses;

pub use config::protected_admin_configured_from_env;
use config::{env_non_empty, parse_admin_route_family_access};
use cookies::{build_cookie, clear_cookie, extract_cookie_value};
use origin_checks::{expected_admin_origin, request_origin};
pub use responses::{parse_login_request, session_created_response, session_deleted_response};

const ADMIN_SESSION_COOKIE: &str = "fitz_admin_session";
const DEFAULT_SESSION_TTL_SECS: i64 = 28_800;
const DEFAULT_OPEN_ADMIN_USERNAME: &str = "admin";
const ADMIN_PUBLIC_ORIGIN_ENV: &str = "FITZ_ADMIN_PUBLIC_ORIGIN";
const ADMIN_ROUTE_FAMILIES_ENV: &str = "FITZ_ADMIN_ROUTE_FAMILIES";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminAuthMode {
    Protected,
    Open,
}

#[derive(Debug, Clone)]
pub struct AdminAuth {
    settings: Arc<Option<AdminAuthSettings>>,
    mode: AdminAuthMode,
}

#[derive(Debug, Clone)]
struct AdminAuthSettings {
    username: String,
    password_hash: String,
    jwt_secret: String,
    session_ttl_secs: i64,
    cookie_secure: bool,
    public_origin: Option<String>,
    route_family_access: AdminRouteFamilyAccess,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdminSessionClaims {
    sub: String,
    role: String,
    #[serde(rename = "routeFamilies", default = "default_route_family_access")]
    route_families: AdminRouteFamilyAccess,
    iat: i64,
    exp: i64,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResponse {
    pub authenticated: bool,
    pub username: String,
    pub route_families: Vec<String>,
    pub route_families_wildcard: bool,
}

#[derive(Debug, Clone)]
pub struct AdminPrincipal {
    pub username: String,
    pub route_family_access: AdminRouteFamilyAccess,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AdminRouteFamilyAccess {
    Wildcard(String),
    Explicit(Vec<String>),
}

impl AdminRouteFamilyAccess {
    pub fn wildcard() -> Self {
        Self::Wildcard("*".to_string())
    }

    pub fn from_env() -> Self {
        parse_admin_route_family_access(env_non_empty(ADMIN_ROUTE_FAMILIES_ENV).as_deref())
    }

    pub fn allows(&self, route_family: &str) -> bool {
        match self {
            Self::Wildcard(value) => value == "*",
            Self::Explicit(values) => values.iter().any(|value| value == route_family),
        }
    }

    pub fn route_families(&self) -> Vec<String> {
        match self {
            Self::Wildcard(_) => Vec::new(),
            Self::Explicit(values) => values.clone(),
        }
    }

    pub fn is_wildcard(&self) -> bool {
        matches!(self, Self::Wildcard(value) if value == "*")
    }
}

impl AdminAuth {
    pub fn from_env() -> Self {
        let mode = match std::env::var("FITZ_ADMIN_AUTH_MODE") {
            Ok(value) if value.eq_ignore_ascii_case("open") => AdminAuthMode::Open,
            _ => AdminAuthMode::Protected,
        };
        let username = env_non_empty("FITZ_ADMIN_USERNAME");
        let password_hash = env_non_empty("FITZ_ADMIN_PASSWORD_HASH");

        let settings = match (username, password_hash) {
            (Some(username), Some(password_hash)) => {
                let jwt_secret = env_non_empty("FITZ_ADMIN_JWT_SECRET")
                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                let session_ttl_secs = std::env::var("FITZ_ADMIN_SESSION_TTL_SECS")
                    .ok()
                    .and_then(|value| value.parse::<i64>().ok())
                    .filter(|value| *value > 0)
                    .unwrap_or(DEFAULT_SESSION_TTL_SECS);

                let cookie_secure = std::env::var("FITZ_ADMIN_COOKIE_SECURE")
                    .ok()
                    .and_then(|value| value.parse::<bool>().ok())
                    .unwrap_or(true);
                let public_origin = std::env::var(ADMIN_PUBLIC_ORIGIN_ENV)
                    .ok()
                    .filter(|value| !value.trim().is_empty());
                let route_family_access = AdminRouteFamilyAccess::from_env();

                tracing::info!(session_ttl_secs, cookie_secure, "Admin auth configured");

                Some(AdminAuthSettings {
                    username,
                    password_hash,
                    jwt_secret,
                    session_ttl_secs,
                    cookie_secure,
                    public_origin,
                    route_family_access,
                })
            }
            _ if matches!(mode, AdminAuthMode::Protected) => {
                tracing::warn!(
                    "Admin auth is not fully configured; session login endpoints will remain unavailable"
                );
                None
            }
            _ => {
                tracing::info!("Admin auth open mode enabled; admin login is not required");
                None
            }
        };

        Self {
            settings: Arc::new(settings),
            mode,
        }
    }

    pub fn is_configured(&self) -> bool {
        self.settings.is_some()
    }

    pub fn auth_mode(&self) -> &'static str {
        match self.mode {
            AdminAuthMode::Protected => "protected",
            AdminAuthMode::Open => "open",
        }
    }

    pub fn login_required(&self) -> bool {
        matches!(self.mode, AdminAuthMode::Protected)
    }

    pub fn authenticate_credentials(
        &self,
        username: &str,
        password: &str,
    ) -> Result<AdminPrincipal, AuthFailure> {
        let settings = self
            .settings
            .as_ref()
            .as_ref()
            .ok_or(AuthFailure::Unavailable)?;

        if username != settings.username {
            return Err(AuthFailure::InvalidCredentials);
        }

        let parsed_hash =
            PasswordHash::new(&settings.password_hash).map_err(|_| AuthFailure::Unavailable)?;

        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .map_err(|_| AuthFailure::InvalidCredentials)?;

        Ok(AdminPrincipal {
            username: settings.username.clone(),
            route_family_access: settings.route_family_access.clone(),
        })
    }

    pub fn issue_session_cookie(&self, principal: &AdminPrincipal) -> Result<String, AuthFailure> {
        let settings = self
            .settings
            .as_ref()
            .as_ref()
            .ok_or(AuthFailure::Unavailable)?;
        let issued_at = Utc::now();
        let expires_at = issued_at + Duration::seconds(settings.session_ttl_secs);
        let claims = AdminSessionClaims {
            sub: principal.username.clone(),
            role: "admin".to_string(),
            route_families: principal.route_family_access.clone(),
            iat: issued_at.timestamp(),
            exp: expires_at.timestamp(),
        };

        let token = jsonwebtoken::encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(settings.jwt_secret.as_bytes()),
        )
        .map_err(|_| AuthFailure::Unavailable)?;

        Ok(build_cookie(
            &token,
            settings.cookie_secure,
            settings.session_ttl_secs,
        ))
    }

    pub fn clear_session_cookie(&self) -> String {
        let secure = self
            .settings
            .as_ref()
            .as_ref()
            .map(|settings| settings.cookie_secure)
            .unwrap_or(true);
        clear_cookie(secure)
    }

    pub fn principal_from_request<B>(
        &self,
        req: &hyper::Request<B>,
    ) -> Result<AdminPrincipal, AuthFailure> {
        if matches!(self.mode, AdminAuthMode::Open) {
            return Ok(AdminPrincipal {
                username: std::env::var("FITZ_ADMIN_OPEN_USERNAME")
                    .unwrap_or_else(|_| DEFAULT_OPEN_ADMIN_USERNAME.to_string()),
                route_family_access: AdminRouteFamilyAccess::wildcard(),
            });
        }

        let settings = self
            .settings
            .as_ref()
            .as_ref()
            .ok_or(AuthFailure::Unavailable)?;
        let token =
            extract_cookie_value(req, ADMIN_SESSION_COOKIE).ok_or(AuthFailure::MissingSession)?;
        let claims = jsonwebtoken::decode::<AdminSessionClaims>(
            &token,
            &DecodingKey::from_secret(settings.jwt_secret.as_bytes()),
            &Validation::default(),
        )
        .map_err(|_| AuthFailure::InvalidSession)?
        .claims;

        if claims.role != "admin" {
            return Err(AuthFailure::InvalidSession);
        }

        Ok(AdminPrincipal {
            username: claims.sub,
            route_family_access: claims.route_families,
        })
    }

    pub fn configured_route_family_access(&self) -> AdminRouteFamilyAccess {
        if matches!(self.mode, AdminAuthMode::Open) {
            return AdminRouteFamilyAccess::wildcard();
        }

        self.settings
            .as_ref()
            .as_ref()
            .map(|settings| settings.route_family_access.clone())
            .unwrap_or_else(AdminRouteFamilyAccess::wildcard)
    }

    pub fn validate_same_origin<B>(&self, req: &hyper::Request<B>) -> Result<(), AuthFailure> {
        if matches!(self.mode, AdminAuthMode::Open) {
            return Ok(());
        }

        let settings = self
            .settings
            .as_ref()
            .as_ref()
            .ok_or(AuthFailure::Unavailable)?;
        let expected = expected_admin_origin(req, settings)?;
        let candidate = request_origin(req)?.ok_or(AuthFailure::Csrf)?;

        if expected.same_origin(&candidate) {
            Ok(())
        } else {
            Err(AuthFailure::Csrf)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AuthFailure {
    Unavailable,
    MissingSession,
    InvalidSession,
    InvalidCredentials,
    Csrf,
}

impl AuthFailure {
    pub fn status_code(self) -> StatusCode {
        match self {
            AuthFailure::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            AuthFailure::MissingSession
            | AuthFailure::InvalidSession
            | AuthFailure::InvalidCredentials => StatusCode::UNAUTHORIZED,
            AuthFailure::Csrf => StatusCode::FORBIDDEN,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            AuthFailure::Unavailable => "Admin authentication is not configured",
            AuthFailure::MissingSession => "Authentication required",
            AuthFailure::InvalidSession => "Invalid or expired session",
            AuthFailure::InvalidCredentials => "Invalid username or password",
            AuthFailure::Csrf => "Admin request origin is not allowed",
        }
    }
}

fn default_route_family_access() -> AdminRouteFamilyAccess {
    AdminRouteFamilyAccess::wildcard()
}

#[cfg(test)]
mod tests;
