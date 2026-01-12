//! Authentication-only errors.
//!
//! Domain authorization decisions live in the domain or session layer.

use std::fmt;

/// Auth-layer errors: token/claims/JWKS issues only.
/// Does not include routing, domain, or authorization decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// Token format is invalid
    InvalidToken(String),
    /// Signature verification failed
    SignatureVerificationFailed(String),
    /// Claims validation failed (issuer, audience, time, tenant resolution)
    ClaimsValidationFailed(String),
    /// JWKS fetch or parsing failed
    JwksError(String),
    /// Permission parsing or extraction failed
    PermissionError(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::InvalidToken(msg) => write!(f, "invalid token: {}", msg),
            AuthError::SignatureVerificationFailed(msg) => {
                write!(f, "signature verification failed: {}", msg)
            }
            AuthError::ClaimsValidationFailed(msg) => {
                write!(f, "claims validation failed: {}", msg)
            }
            AuthError::JwksError(msg) => write!(f, "jwks error: {}", msg),
            AuthError::PermissionError(msg) => write!(f, "permission error: {}", msg),
        }
    }
}

impl std::error::Error for AuthError {}

impl From<AuthError> for String {
    fn from(err: AuthError) -> Self {
        err.to_string()
    }
}
