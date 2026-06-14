use crate::api::http::{Body, Response};
use argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
};
use chrono::{Duration, Utc};
use http_body_util::BodyExt;
use hyper::header::{COOKIE, SET_COOKIE};
use hyper::StatusCode;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

const ADMIN_SESSION_COOKIE: &str = "fitz_admin_session";
const DEFAULT_SESSION_TTL_SECS: i64 = 28_800;
const DEFAULT_OPEN_ADMIN_USERNAME: &str = "admin";
const ADMIN_PUBLIC_ORIGIN_ENV: &str = "FITZ_ADMIN_PUBLIC_ORIGIN";

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdminSessionClaims {
    sub: String,
    role: String,
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
}

#[derive(Debug, Clone)]
pub struct AdminPrincipal {
    pub username: String,
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

                tracing::info!(session_ttl_secs, cookie_secure, "Admin auth configured");

                Some(AdminAuthSettings {
                    username,
                    password_hash,
                    jwt_secret,
                    session_ttl_secs,
                    cookie_secure,
                    public_origin,
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
        })
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

pub fn protected_admin_configured_from_env() -> bool {
    let mode = match std::env::var("FITZ_ADMIN_AUTH_MODE") {
        Ok(value) if value.eq_ignore_ascii_case("open") => AdminAuthMode::Open,
        _ => AdminAuthMode::Protected,
    };

    if matches!(mode, AdminAuthMode::Open) {
        return false;
    }

    env_non_empty("FITZ_ADMIN_USERNAME").is_some()
        && env_non_empty("FITZ_ADMIN_PASSWORD_HASH").is_some()
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

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn request_header<'a, B>(req: &'a hyper::Request<B>, name: &str) -> Option<&'a str> {
    req.headers().get(name)?.to_str().ok()
}

fn single_header_value<'a, B>(
    req: &'a hyper::Request<B>,
    name: &str,
) -> Result<Option<&'a str>, AuthFailure> {
    let mut values = req.headers().get_all(name).iter();
    let Some(first) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(AuthFailure::Csrf);
    }
    first.to_str().map(Some).map_err(|_| AuthFailure::Csrf)
}

fn request_origin<B>(
    req: &hyper::Request<B>,
) -> Result<Option<crate::api::origin::ExactOrigin>, AuthFailure> {
    let origin = single_header_value(req, "origin")?;
    let referer = single_header_value(req, "referer")?;
    let parsed_referer = referer
        .map(crate::api::origin::parse_url_origin)
        .transpose()
        .map_err(|_| AuthFailure::Csrf)?;

    let Some(origin) = origin else {
        return Ok(parsed_referer);
    };

    let parsed_origin =
        crate::api::origin::parse_exact_origin(origin).map_err(|_| AuthFailure::Csrf)?;

    if parsed_referer
        .as_ref()
        .is_some_and(|referer| !parsed_origin.same_origin(referer))
    {
        return Err(AuthFailure::Csrf);
    }

    Ok(Some(parsed_origin))
}

fn expected_admin_origin<B>(
    req: &hyper::Request<B>,
    settings: &AdminAuthSettings,
) -> Result<crate::api::origin::ExactOrigin, AuthFailure> {
    if let Some(origin) = &settings.public_origin {
        return crate::api::origin::parse_exact_origin(origin).map_err(|_| AuthFailure::Csrf);
    }

    let host = request_header(req, "host").ok_or(AuthFailure::Csrf)?;
    let proto = request_header(req, "x-forwarded-proto")
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("http");
    crate::api::origin::parse_exact_origin(&format!("{proto}://{host}"))
        .map_err(|_| AuthFailure::Csrf)
}

pub async fn parse_login_request<B>(req: hyper::Request<B>) -> Result<LoginRequest, AuthFailure>
where
    B: hyper::body::Body,
{
    let body = req
        .into_body()
        .collect()
        .await
        .map_err(|_| AuthFailure::InvalidCredentials)?
        .to_bytes();
    serde_json::from_slice::<LoginRequest>(&body).map_err(|_| AuthFailure::InvalidCredentials)
}

pub fn session_created_response(set_cookie: &str) -> Response {
    hyper::http::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(SET_COOKIE, set_cookie)
        .body(Body::default())
        .unwrap()
}

pub fn session_deleted_response(clear_cookie_header: &str) -> Response {
    hyper::http::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(SET_COOKIE, clear_cookie_header)
        .body(Body::default())
        .unwrap()
}

fn extract_cookie_value<B>(req: &hyper::Request<B>, cookie_name: &str) -> Option<String> {
    let cookie_header = req.headers().get(COOKIE)?.to_str().ok()?;
    cookie_header.split(';').find_map(|cookie| {
        let trimmed = cookie.trim();
        let (name, value) = trimmed.split_once('=')?;
        if name == cookie_name {
            Some(value.to_string())
        } else {
            None
        }
    })
}

fn build_cookie(token: &str, secure: bool, max_age: i64) -> String {
    let mut cookie = format!(
        "{}={}; HttpOnly; Path=/; SameSite=Strict; Max-Age={}",
        ADMIN_SESSION_COOKIE, token, max_age
    );

    if secure {
        cookie.push_str("; Secure");
    }

    cookie
}

fn clear_cookie(secure: bool) -> String {
    let mut cookie = format!(
        "{}=; HttpOnly; Path=/; SameSite=Strict; Max-Age=0",
        ADMIN_SESSION_COOKIE
    );
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::{
        password_hash::{rand_core::OsRng, SaltString},
        PasswordHasher,
    };
    use serial_test::serial;

    fn password_hash_for(password: &str) -> String {
        let salt = SaltString::generate(&mut OsRng);
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .unwrap()
            .to_string()
    }

    fn reset_admin_env() {
        for key in [
            "FITZ_ADMIN_AUTH_MODE",
            "FITZ_ADMIN_USERNAME",
            "FITZ_ADMIN_PASSWORD_HASH",
            "FITZ_ADMIN_OPEN_USERNAME",
            "FITZ_ADMIN_COOKIE_SECURE",
            "FITZ_ADMIN_PUBLIC_ORIGIN",
        ] {
            std::env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn should_authenticate_with_valid_credentials() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));

        // Act
        let auth = AdminAuth::from_env();
        let principal = auth.authenticate_credentials("admin", "pwd123");

        // Assert
        assert!(principal.is_ok());
    }

    #[test]
    #[serial]
    fn should_configure_protected_admin_without_jwt_secret() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
        std::env::remove_var("FITZ_ADMIN_JWT_SECRET");

        // Act
        let auth = AdminAuth::from_env();

        // Assert
        assert!(auth.is_configured());
        assert!(protected_admin_configured_from_env());
    }

    #[test]
    #[serial]
    fn should_reject_cookie_signed_by_different_admin_auth_instance() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
        std::env::remove_var("FITZ_ADMIN_JWT_SECRET");

        let issuing_auth = AdminAuth::from_env();
        let principal = issuing_auth
            .authenticate_credentials("admin", "pwd123")
            .expect("admin principal");
        let cookie = issuing_auth.issue_session_cookie(&principal).unwrap();
        let cookie_value = cookie.split(';').next().unwrap().to_string();
        let req = hyper::http::Request::builder()
            .header(COOKIE, cookie_value)
            .body(Body::default())
            .unwrap();

        // Act
        let validating_auth = AdminAuth::from_env();
        let extracted = validating_auth.principal_from_request(&req);

        // Assert
        assert!(matches!(extracted, Err(AuthFailure::InvalidSession)));
    }

    #[test]
    #[serial]
    fn should_extract_principal_from_cookie() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));

        let auth = AdminAuth::from_env();
        let principal = auth.authenticate_credentials("admin", "pwd123").unwrap();
        let cookie = auth.issue_session_cookie(&principal).unwrap();
        let cookie_value = cookie.split(';').next().unwrap().to_string();

        let req = hyper::http::Request::builder()
            .header(COOKIE, cookie_value)
            .body(Body::default())
            .unwrap();

        // Act
        let extracted = auth.principal_from_request(&req).unwrap();

        // Assert
        assert_eq!(extracted.username, "admin");
    }

    #[test]
    #[serial]
    fn should_allow_open_admin_without_credentials() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_AUTH_MODE", "open");

        let auth = AdminAuth::from_env();
        let req = hyper::http::Request::builder()
            .body(Body::default())
            .unwrap();

        // Act
        let principal = auth
            .principal_from_request(&req)
            .expect("open mode principal");

        // Assert
        assert_eq!(auth.auth_mode(), "open");
        assert!(!auth.login_required());
        assert_eq!(principal.username, "admin");
    }

    #[test]
    #[serial]
    fn should_mark_admin_session_cookie_secure_by_default() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
        let auth = AdminAuth::from_env();
        let principal = auth.authenticate_credentials("admin", "pwd123").unwrap();

        // Act
        let cookie = auth.issue_session_cookie(&principal).unwrap();

        // Assert
        assert!(cookie.contains("; Secure"));
        assert!(cookie.contains("; SameSite=Strict"));
    }

    #[test]
    #[serial]
    fn should_allow_admin_session_cookie_secure_opt_out() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
        std::env::set_var("FITZ_ADMIN_COOKIE_SECURE", "false");
        let auth = AdminAuth::from_env();
        let principal = auth.authenticate_credentials("admin", "pwd123").unwrap();

        // Act
        let cookie = auth.issue_session_cookie(&principal).unwrap();

        // Assert
        assert!(!cookie.contains("; Secure"));
        assert!(cookie.contains("; SameSite=Strict"));
    }

    #[test]
    #[serial]
    fn should_clear_admin_session_cookie_with_matching_secure_attributes() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
        let auth = AdminAuth::from_env();

        // Act
        let cookie = auth.clear_session_cookie();

        // Assert
        assert!(cookie.contains("fitz_admin_session=;"));
        assert!(cookie.contains("; HttpOnly"));
        assert!(cookie.contains("; Secure"));
        assert!(cookie.contains("; SameSite=Strict"));
        assert!(cookie.contains("; Max-Age=0"));
    }

    #[test]
    #[serial]
    fn should_validate_same_origin_for_protected_admin_request() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
        std::env::set_var("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com");
        let auth = AdminAuth::from_env();
        let same_origin = hyper::http::Request::builder()
            .header("origin", "https://admin.example.com")
            .body(Body::default())
            .unwrap();
        let cross_origin = hyper::http::Request::builder()
            .header("origin", "https://evil.example.com")
            .body(Body::default())
            .unwrap();

        // Act
        let allowed = auth.validate_same_origin(&same_origin);
        let denied = auth.validate_same_origin(&cross_origin);

        // Assert
        assert!(allowed.is_ok());
        assert!(matches!(denied, Err(AuthFailure::Csrf)));
    }

    #[test]
    #[serial]
    fn should_validate_same_origin_referer_for_protected_admin_request() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
        std::env::set_var("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com");
        let auth = AdminAuth::from_env();
        let same_origin = hyper::http::Request::builder()
            .header("referer", "https://admin.example.com/settings?tab=security")
            .body(Body::default())
            .unwrap();

        // Act
        let result = auth.validate_same_origin(&same_origin);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn should_reject_duplicate_origin_headers_for_protected_admin_request() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
        std::env::set_var("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com");
        let auth = AdminAuth::from_env();
        let req = hyper::http::Request::builder()
            .header("origin", "https://admin.example.com")
            .header("origin", "https://evil.example.com")
            .body(Body::default())
            .unwrap();

        // Act
        let result = auth.validate_same_origin(&req);

        // Assert
        assert!(matches!(result, Err(AuthFailure::Csrf)));
    }

    #[test]
    #[serial]
    fn should_reject_duplicate_referer_headers_for_protected_admin_request() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
        std::env::set_var("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com");
        let auth = AdminAuth::from_env();
        let req = hyper::http::Request::builder()
            .header("referer", "https://admin.example.com/settings")
            .header("referer", "https://evil.example.com/settings")
            .body(Body::default())
            .unwrap();

        // Act
        let result = auth.validate_same_origin(&req);

        // Assert
        assert!(matches!(result, Err(AuthFailure::Csrf)));
    }

    #[test]
    #[serial]
    fn should_reject_conflicting_origin_and_referer_for_protected_admin_request() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
        std::env::set_var("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com");
        let auth = AdminAuth::from_env();
        let req = hyper::http::Request::builder()
            .header("origin", "https://admin.example.com")
            .header("referer", "https://evil.example.com/settings")
            .body(Body::default())
            .unwrap();

        // Act
        let result = auth.validate_same_origin(&req);

        // Assert
        assert!(matches!(result, Err(AuthFailure::Csrf)));
    }

    #[test]
    #[serial]
    fn should_validate_same_origin_given_matching_origin_and_referer() {
        // Arrange
        reset_admin_env();
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
        std::env::set_var("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com");
        let auth = AdminAuth::from_env();
        let req = hyper::http::Request::builder()
            .header("origin", "https://admin.example.com")
            .header("referer", "https://admin.example.com/settings")
            .body(Body::default())
            .unwrap();

        // Act
        let result = auth.validate_same_origin(&req);

        // Assert
        assert!(result.is_ok());
    }
}
