//! Authentication helpers for Fitz
//!
// This module implements the simple token issuance and environment-driven
// authentication modes used by the project.  It intentionally keeps a tiny
// surface (no heavy OIDC/crypto plumbing) as a baseline which can be
// replaced by proper JWKS/JWT handling later.

use crate::config;

/// Issue a token for a client using client credentials from env.
///
/// This is intentionally simple: it returns a `mock:` token understood by the
/// existing `authz::mock_jwks::validate_mock_token` parser.  If the configured
/// client id/secret don't match, returns `None`.
pub fn issue_token_for_client(client_id: &str, client_secret: &str) -> Option<String> {
    let cfg = config::load();

    // If NO_AUTH enabled, return a token for the dev tenant
    if cfg.auth.no_auth {
        return Some("mock:dev".to_string());
    }

    if let (Some(cfg_id), Some(cfg_secret)) = (cfg.auth.client_id.clone(), cfg.auth.client_secret.clone()) {
        if client_id == cfg_id && client_secret == cfg_secret {
            let mut token = format!("mock:{}:control", client_id);
            if let Some(perms) = cfg.auth.client_permissions.clone() {
                if !perms.is_empty() {
                    token = format!("{}|{}", token, perms.join(","));
                }
            }
            return Some(token);
        }
    }
    None
}

/// Return whether NO_AUTH mode is enabled.
pub fn no_auth_enabled() -> bool {
    config::load().auth.no_auth
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[serial_test::serial]
    fn should_issue_token_when_client_creds_match_env() {
        // Arrange
        std::env::remove_var("FITZ_NO_AUTH");
        std::env::set_var("FITZ_CLIENT_ID", "node-1");
        std::env::set_var("FITZ_CLIENT_SECRET", "s3cr3t");
        std::env::set_var("FITZ_CLIENT_PERMISSIONS", "read:stream://acme/*,write:queue://acme/orders/*");

        // Act
        let token = issue_token_for_client("node-1", "s3cr3t");

        // Assert
        assert!(token.is_some());
        let t = token.unwrap();
        assert!(t.starts_with("mock:node-1:control"));
        assert!(t.contains("read:stream"));
    }

    #[test]
    #[serial_test::serial]
    fn should_not_issue_when_creds_invalid() {
        // Arrange
        std::env::set_var("FITZ_NO_AUTH", "0");
        std::env::remove_var("FITZ_CLIENT_ID");
        std::env::remove_var("FITZ_CLIENT_SECRET");

        // Act
        let token = issue_token_for_client("x", "y");

        // Assert
        assert!(token.is_none());
    }

    #[test]
    #[serial_test::serial]
    fn no_auth_mode_allows_dev_token() {
        // Arrange
        std::env::set_var("FITZ_NO_AUTH", "1");
        std::env::remove_var("FITZ_CLIENT_ID");
        std::env::remove_var("FITZ_CLIENT_SECRET");

        // Act
        let token = issue_token_for_client("foo", "bar");

        // Assert
        assert_eq!(token, Some("mock:dev".to_string()));
    }
}
// todo : the control plane will need to issue jwt to other nodes
