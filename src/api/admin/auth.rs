use argon2::{
    password_hash::{PasswordHash, PasswordVerifier, SaltString},
    Argon2, PasswordHasher,
};
use chrono::{Duration, Utc};
use hyper::StatusCode;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
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
const ROOT_USERNAME: &str = "root";
const ROOT_PASSWORD_ENV: &str = "FITZ_ROOT_PASSWORD";
const ADMIN_PUBLIC_ORIGIN_ENV: &str = "FITZ_ADMIN_PUBLIC_ORIGIN";
const ADMIN_ROUTE_FAMILIES_ENV: &str = "FITZ_ADMIN_ROUTE_FAMILIES";
const ADMIN_LOGIN_ATTEMPT_LIMIT: u32 = 5;
const ADMIN_LOGIN_WINDOW: StdDuration = StdDuration::from_mins(1);
const ADMIN_LOGIN_CLIENT_LIMIT: usize = 4_096;

#[derive(Clone, Copy, Debug)]
pub(crate) struct AdminClientIp(pub IpAddr);

impl Default for AdminClientIp {
    fn default() -> Self {
        Self(IpAddr::V4(Ipv4Addr::LOCALHOST))
    }
}

#[derive(Debug)]
struct LoginAttemptWindow {
    started_at: Instant,
    attempts: u32,
}

#[derive(Debug, Default)]
struct LoginRateLimiter {
    clients: HashMap<IpAddr, LoginAttemptWindow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminAuthMode {
    Protected,
    Open,
}

#[derive(Debug, Clone)]
pub struct AdminAuth {
    settings: Arc<Option<AdminAuthSettings>>,
    mode: AdminAuthMode,
    provisioned_route_families: Arc<parking_lot::RwLock<Option<Vec<u32>>>>,
    login_rate_limiter: Arc<parking_lot::Mutex<LoginRateLimiter>>,
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
    #[must_use]
    pub fn wildcard() -> Self {
        Self::Wildcard("*".to_string())
    }

    #[must_use]
    pub fn from_env() -> Self {
        parse_admin_route_family_access(env_non_empty(ADMIN_ROUTE_FAMILIES_ENV).as_deref())
    }

    #[must_use]
    pub fn allows(&self, route_family: &str) -> bool {
        match self {
            Self::Wildcard(value) => value == "*",
            Self::Explicit(values) => values.iter().any(|value| value == route_family),
        }
    }

    #[must_use]
    pub fn route_families(&self) -> Vec<String> {
        match self {
            Self::Wildcard(_) => Vec::new(),
            Self::Explicit(values) => values.clone(),
        }
    }

    #[must_use]
    pub fn is_wildcard(&self) -> bool {
        matches!(self, Self::Wildcard(value) if value == "*")
    }

    fn is_valid_grant_set(&self) -> bool {
        match self {
            Self::Wildcard(value) => value == "*",
            Self::Explicit(values) => {
                !values.is_empty()
                    && values.iter().all(|value| {
                        value
                            .parse::<u32>()
                            .is_ok_and(|family| family.to_string() == *value)
                    })
            }
        }
    }
}

impl AdminAuth {
    pub fn from_env() -> Self {
        let mode = match std::env::var("FITZ_ADMIN_AUTH_MODE") {
            Ok(value) if value.eq_ignore_ascii_case("open") => AdminAuthMode::Open,
            _ => AdminAuthMode::Protected,
        };
        let root_password = env_non_empty(ROOT_PASSWORD_ENV);

        let settings = match root_password {
            Some(root_password) => {
                let password_hash = (|| {
                    let mut salt_bytes = [0_u8; 16];
                    getrandom::fill(&mut salt_bytes)
                        .map_err(|error| format!("Failed to generate password salt: {error}"))?;
                    let salt = SaltString::encode_b64(&salt_bytes)
                        .map_err(|error| format!("Failed to encode password salt: {error}"))?;
                    Argon2::default()
                        .hash_password(root_password.as_bytes(), &salt)
                        .map(|hash| hash.to_string())
                        .map_err(|error| format!("Failed to hash password: {error}"))
                })();
                if let Err(error) = &password_hash {
                    tracing::error!(%error, "Failed to hash the configured Fitz root password");
                }
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

                password_hash.ok().map(|password_hash| AdminAuthSettings {
                    username: ROOT_USERNAME.to_string(),
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
            provisioned_route_families: Arc::new(parking_lot::RwLock::new(None)),
            login_rate_limiter: Arc::new(parking_lot::Mutex::new(LoginRateLimiter::default())),
        }
    }

    /// Restrict explicit admin grants to the route-family set provisioned at
    /// broker startup. `*` remains the only wildcard grant.
    pub fn set_provisioned_route_families(&self, route_families: &[u32]) {
        *self.provisioned_route_families.write() = Some(route_families.to_vec());
    }

    #[must_use]
    pub fn provisioned_route_families(&self) -> Vec<String> {
        self.provisioned_route_families
            .read()
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(u32::to_string)
            .collect()
    }

    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.settings.is_some()
    }

    #[must_use]
    pub fn auth_mode(&self) -> &'static str {
        match self.mode {
            AdminAuthMode::Protected => "protected",
            AdminAuthMode::Open => "open",
        }
    }

    #[must_use]
    pub fn login_required(&self) -> bool {
        matches!(self.mode, AdminAuthMode::Protected)
    }

    /// Authenticates admin credentials against the configured password hash.
    ///
    /// # Errors
    ///
    /// Returns [`AuthFailure::Unavailable`] when admin auth is not configured or
    /// the stored password hash cannot be parsed. Returns
    /// [`AuthFailure::InvalidCredentials`] when the username or password does not
    /// match.
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

        if !self.valid_route_family_grants(&settings.route_family_access) {
            return Err(AuthFailure::Unavailable);
        }

        let parsed_hash =
            PasswordHash::new(&settings.password_hash).map_err(|_| AuthFailure::Unavailable)?;

        let password_matches = Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok();
        if username != settings.username || !password_matches {
            return Err(AuthFailure::InvalidCredentials);
        }

        Ok(AdminPrincipal {
            username: settings.username.clone(),
            route_family_access: settings.route_family_access.clone(),
        })
    }

    pub(crate) fn begin_login_attempt(&self, client: AdminClientIp) -> Result<(), AuthFailure> {
        let now = Instant::now();
        let mut limiter = self.login_rate_limiter.lock();
        limiter
            .clients
            .retain(|_, window| now.duration_since(window.started_at) < ADMIN_LOGIN_WINDOW);

        if !limiter.clients.contains_key(&client.0)
            && limiter.clients.len() >= ADMIN_LOGIN_CLIENT_LIMIT
        {
            return Err(AuthFailure::RateLimited);
        }

        let window = limiter
            .clients
            .entry(client.0)
            .or_insert(LoginAttemptWindow {
                started_at: now,
                attempts: 0,
            });
        if now.duration_since(window.started_at) >= ADMIN_LOGIN_WINDOW {
            window.started_at = now;
            window.attempts = 0;
        }
        if window.attempts >= ADMIN_LOGIN_ATTEMPT_LIMIT {
            return Err(AuthFailure::RateLimited);
        }
        window.attempts = window.attempts.saturating_add(1);
        Ok(())
    }

    pub(crate) fn complete_login_attempt(&self, client: AdminClientIp, succeeded: bool) {
        if succeeded {
            self.login_rate_limiter.lock().clients.remove(&client.0);
        }
    }

    /// Issues a signed admin session cookie for the authenticated principal.
    ///
    /// # Errors
    ///
    /// Returns [`AuthFailure::Unavailable`] when admin auth is not configured or
    /// the session token cannot be encoded.
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

    #[must_use]
    pub fn clear_session_cookie(&self) -> String {
        let secure = self
            .settings
            .as_ref()
            .as_ref()
            .is_none_or(|settings| settings.cookie_secure);
        clear_cookie(secure)
    }

    /// Extracts and validates the admin principal from the incoming request.
    ///
    /// # Errors
    ///
    /// Returns [`AuthFailure::Unavailable`] when admin auth is not configured,
    /// [`AuthFailure::MissingSession`] when the session cookie is absent, and
    /// [`AuthFailure::InvalidSession`] when the token is invalid or has the wrong
    /// role.
    pub fn principal_from_request<B>(
        &self,
        req: &hyper::Request<B>,
    ) -> Result<AdminPrincipal, AuthFailure> {
        if matches!(self.mode, AdminAuthMode::Open) {
            return Ok(AdminPrincipal {
                username: ROOT_USERNAME.to_string(),
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

        if !self.valid_route_family_grants(&claims.route_families) {
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
            .map_or_else(AdminRouteFamilyAccess::wildcard, |settings| {
                settings.route_family_access.clone()
            })
    }

    fn valid_route_family_grants(&self, access: &AdminRouteFamilyAccess) -> bool {
        if !access.is_valid_grant_set() {
            return false;
        }
        let Some(provisioned) = self.provisioned_route_families.read().clone() else {
            return true;
        };
        match access {
            AdminRouteFamilyAccess::Wildcard(value) => value == "*",
            AdminRouteFamilyAccess::Explicit(values) => values.iter().all(|value| {
                value
                    .parse::<u32>()
                    .ok()
                    .is_some_and(|family| provisioned.contains(&family))
            }),
        }
    }

    #[must_use]
    pub fn is_provisioned_route_family(&self, family: u32) -> bool {
        self.provisioned_route_families
            .read()
            .as_ref()
            .is_none_or(|families| families.contains(&family))
    }

    /// Verifies the request origin against the configured admin origin policy.
    ///
    /// # Errors
    ///
    /// Returns [`AuthFailure::Unavailable`] when admin auth is not configured and
    /// [`AuthFailure::Csrf`] when the request origin does not match the expected
    /// admin origin.
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
    RateLimited,
    Csrf,
}

impl AuthFailure {
    #[must_use]
    pub fn status_code(self) -> StatusCode {
        match self {
            AuthFailure::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            AuthFailure::MissingSession
            | AuthFailure::InvalidSession
            | AuthFailure::InvalidCredentials => StatusCode::UNAUTHORIZED,
            AuthFailure::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            AuthFailure::Csrf => StatusCode::FORBIDDEN,
        }
    }

    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            AuthFailure::Unavailable => "Admin authentication is not configured",
            AuthFailure::MissingSession => "Authentication required",
            AuthFailure::InvalidSession => "Invalid or expired session",
            AuthFailure::InvalidCredentials => "Invalid username or password",
            AuthFailure::RateLimited => "Too many login attempts",
            AuthFailure::Csrf => "Admin request origin is not allowed",
        }
    }
}

fn default_route_family_access() -> AdminRouteFamilyAccess {
    AdminRouteFamilyAccess::wildcard()
}

#[cfg(test)]
mod tests;
