//! Authorization module (renamed from auth)

pub mod mock_jwks;
pub mod permissions;
pub mod tokens;

/// Validate a token using the mock JWKS validator. Returns Some(tenant) on success.
pub fn validate_token(token: &str) -> Option<String> {
    // If NO_AUTH is enabled, accept every token as belonging to the dev tenant.
    if crate::config::load().auth.no_auth {
        return Some("dev".to_string());
    }

    // Allow other authenticators to participate in the validation flow in future
    // (OIDC) — for now we continue to accept the local mock token format.
    if let Some(claims) = mock_jwks::validate_mock_token(token) {
        // Use aud as tenant when present, otherwise derive from sub
        Some(claims.aud.unwrap_or(claims.sub))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_accept_any_token_when_no_auth() {
        std::env::set_var("FITZ_NO_AUTH", "1");
        let val = validate_token("anything");
        assert_eq!(val, Some("dev".to_string()));
    }
}

/// Initialize authorization subsystem (stub)
pub fn init() {
    // TODO: init authz backends
}
