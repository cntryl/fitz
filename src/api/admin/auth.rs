use argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
};
use chrono::{Duration, Utc};
use hyper::header::{COOKIE, SET_COOKIE};
use hyper::{Body, Request, Response, StatusCode};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const ADMIN_SESSION_COOKIE: &str = "fitz_admin_session";
const DEFAULT_SESSION_TTL_SECS: i64 = 28_800;

#[derive(Debug, Clone)]
pub struct AdminAuth {
    settings: Arc<Option<AdminAuthSettings>>,
}

#[derive(Debug, Clone)]
struct AdminAuthSettings {
    username: String,
    password_hash: String,
    jwt_secret: String,
    session_ttl_secs: i64,
    cookie_secure: bool,
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
        let username = std::env::var("FITZ_ADMIN_USERNAME").ok();
        let password_hash = std::env::var("FITZ_ADMIN_PASSWORD_HASH").ok();
        let jwt_secret = std::env::var("FITZ_ADMIN_JWT_SECRET").ok();

        let settings = match (username, password_hash, jwt_secret) {
            (Some(username), Some(password_hash), Some(jwt_secret)) => {
                let session_ttl_secs = std::env::var("FITZ_ADMIN_SESSION_TTL_SECS")
                    .ok()
                    .and_then(|value| value.parse::<i64>().ok())
                    .filter(|value| *value > 0)
                    .unwrap_or(DEFAULT_SESSION_TTL_SECS);

                let cookie_secure = std::env::var("FITZ_ADMIN_COOKIE_SECURE")
                    .ok()
                    .and_then(|value| value.parse::<bool>().ok())
                    .unwrap_or(false);

                tracing::info!(session_ttl_secs, cookie_secure, "Admin auth configured");

                Some(AdminAuthSettings {
                    username,
                    password_hash,
                    jwt_secret,
                    session_ttl_secs,
                    cookie_secure,
                })
            }
            _ => {
                tracing::warn!(
                    "Admin auth is not fully configured; session login endpoints will remain unavailable"
                );
                None
            }
        };

        Self {
            settings: Arc::new(settings),
        }
    }

    pub fn is_configured(&self) -> bool {
        self.settings.is_some()
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
        clear_cookie()
    }

    pub fn principal_from_request(
        &self,
        req: &Request<Body>,
    ) -> Result<AdminPrincipal, AuthFailure> {
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
}

#[derive(Debug, Clone, Copy)]
pub enum AuthFailure {
    Unavailable,
    MissingSession,
    InvalidSession,
    InvalidCredentials,
}

impl AuthFailure {
    pub fn status_code(self) -> StatusCode {
        match self {
            AuthFailure::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            AuthFailure::MissingSession
            | AuthFailure::InvalidSession
            | AuthFailure::InvalidCredentials => StatusCode::UNAUTHORIZED,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            AuthFailure::Unavailable => "Admin authentication is not configured",
            AuthFailure::MissingSession => "Authentication required",
            AuthFailure::InvalidSession => "Invalid or expired session",
            AuthFailure::InvalidCredentials => "Invalid username or password",
        }
    }
}

pub async fn parse_login_request(req: Request<Body>) -> Result<LoginRequest, AuthFailure> {
    let body = hyper::body::to_bytes(req.into_body())
        .await
        .map_err(|_| AuthFailure::InvalidCredentials)?;
    serde_json::from_slice::<LoginRequest>(&body).map_err(|_| AuthFailure::InvalidCredentials)
}

pub fn session_created_response(set_cookie: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(SET_COOKIE, set_cookie)
        .body(Body::empty())
        .unwrap()
}

pub fn session_deleted_response(clear_cookie_header: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(SET_COOKIE, clear_cookie_header)
        .body(Body::empty())
        .unwrap()
}

fn extract_cookie_value(req: &Request<Body>, cookie_name: &str) -> Option<String> {
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
        "{}={}; HttpOnly; Path=/; SameSite=Lax; Max-Age={}",
        ADMIN_SESSION_COOKIE, token, max_age
    );

    if secure {
        cookie.push_str("; Secure");
    }

    cookie
}

fn clear_cookie() -> String {
    format!(
        "{}=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0",
        ADMIN_SESSION_COOKIE
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::{
        password_hash::{rand_core::OsRng, SaltString},
        PasswordHasher,
    };

    fn password_hash_for(password: &str) -> String {
        let salt = SaltString::generate(&mut OsRng);
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .unwrap()
            .to_string()
    }

    #[test]
    fn should_authenticate_with_valid_credentials() {
        // Arrange
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var(
            "FITZ_ADMIN_PASSWORD_HASH",
            password_hash_for("pwd123"),
        );
        std::env::set_var("FITZ_ADMIN_JWT_SECRET", "jwt-secret");

        // Act
        let auth = AdminAuth::from_env();
        let principal = auth.authenticate_credentials("admin", "pwd123");

        // Assert
        assert!(principal.is_ok());
    }

    #[test]
    fn should_extract_principal_from_cookie() {
        // Arrange
        std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
        std::env::set_var(
            "FITZ_ADMIN_PASSWORD_HASH",
            password_hash_for("pwd123"),
        );
        std::env::set_var("FITZ_ADMIN_JWT_SECRET", "jwt-secret");

        let auth = AdminAuth::from_env();
        let principal = auth
            .authenticate_credentials("admin", "pwd123")
            .unwrap();
        let cookie = auth.issue_session_cookie(&principal).unwrap();
        let cookie_value = cookie.split(';').next().unwrap().to_string();

        let req = Request::builder()
            .header(COOKIE, cookie_value)
            .body(Body::empty())
            .unwrap();

        // Act
        let extracted = auth.principal_from_request(&req).unwrap();

        // Assert
        assert_eq!(extracted.username, "admin");
    }
}
