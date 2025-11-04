//! Authorization module (renamed from auth)

pub mod mock_jwks;
pub mod permissions;
pub mod tokens;

/// Validate a token using the mock JWKS validator. Returns Some(tenant) on success.
pub fn validate_token(token: &str) -> Option<String> {
    if let Some(claims) = mock_jwks::validate_mock_token(token) {
        // Use aud as tenant when present, otherwise derive from sub
        Some(claims.aud.unwrap_or(claims.sub))
    } else {
        None
    }
}

/// Initialize authorization subsystem (stub)
pub fn init() {
    // TODO: init authz backends
}
