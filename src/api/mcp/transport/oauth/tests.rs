use super::*;
use crate::api::admin::auth::AdminRouteFamilyAccess;
use crate::api::mcp::McpCapabilityClass;
use crate::auth::Access;
use crate::boot::Runtime;
use crate::runtime::Router;
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::Serialize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const ISSUER: &str = "https://identity.example.test";
const AUDIENCE: &str = "fitz-mcp-tests";
const PUBLIC_URL: &str = "https://fitz.example.test/mcp";

#[derive(Clone, Serialize)]
struct TestClaims {
    iss: String,
    sub: String,
    aud: String,
    iat: u64,
    exp: u64,
    fitz_role: String,
    fitz_route_families: AdminRouteFamilyAccess,
    fitz_permissions: Vec<String>,
    fitz_mcp_capabilities: Vec<McpCapabilityClass>,
    scope: String,
}

fn claims() -> TestClaims {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    TestClaims {
        iss: ISSUER.to_string(),
        sub: "operator-17".to_string(),
        aud: AUDIENCE.to_string(),
        iat: now,
        exp: now + 600,
        fitz_role: "admin".to_string(),
        fitz_route_families: AdminRouteFamilyAccess::wildcard(),
        fitz_permissions: vec!["kv://**#read".to_string()],
        fitz_mcp_capabilities: vec![McpCapabilityClass::Summary, McpCapabilityClass::Inspect],
        scope: "fitz.mcp.read".to_string(),
    }
}

fn runtime() -> Runtime {
    Runtime::new(Arc::new(Router::new()))
}

fn verifier() -> McpOAuthVerifier {
    McpOAuthVerifier::new(
        ISSUER.to_string(),
        AUDIENCE.to_string(),
        PUBLIC_URL,
        include_bytes!("../testdata/insecure_test_only_rsa_public.pem"),
        None,
    )
    .expect("test OAuth verifier")
}

fn bearer(claims: &TestClaims) -> hyper::header::HeaderValue {
    let key = EncodingKey::from_rsa_pem(include_bytes!(
        "../testdata/insecure_test_only_rsa_private.pem"
    ))
    .expect("test signing key");
    let token = encode(&Header::new(Algorithm::RS256), claims, &key).expect("signed test token");
    hyper::header::HeaderValue::from_str(&format!("Bearer {token}")).expect("bearer header")
}

#[test]
fn should_authenticate_valid_rs256_bearer_and_provision_mcp_capabilities() {
    // Arrange
    let claims = claims();
    let authorization = bearer(&claims);
    let verifier = verifier();

    // Act
    let authenticated = verifier
        .authenticate(Some(&authorization), &runtime(), McpAuditBuffer::new())
        .expect("valid bearer token");

    // Assert
    assert_eq!(
        authenticated.context.principal_name().as_deref(),
        Some("operator-17")
    );
    assert!(authenticated.policy.allows(McpCapabilityClass::Summary));
    assert!(authenticated.policy.allows(McpCapabilityClass::Inspect));
    assert!(authenticated
        .context
        .permissions
        .allows_route("kv://realm/area/resource", Access::Read));
}

#[test]
fn should_reject_wrong_audience_in_signed_bearer_token() {
    // Arrange
    let mut claims = claims();
    claims.aud = "other-resource".to_string();
    let authorization = bearer(&claims);

    // Act
    let result = verifier().authenticate(Some(&authorization), &runtime(), McpAuditBuffer::new());

    // Assert
    assert!(matches!(result, Err(AuthError::InvalidToken)));
}

#[test]
fn should_reject_expired_signed_bearer_token() {
    // Arrange
    let mut claims = claims();
    claims.exp = claims.iat.saturating_sub(1);
    let authorization = bearer(&claims);

    // Act
    let result = verifier().authenticate(Some(&authorization), &runtime(), McpAuditBuffer::new());

    // Assert
    assert!(matches!(result, Err(AuthError::InvalidToken)));
}

#[test]
fn should_reject_capability_claim_without_required_oauth_scope() {
    // Arrange
    let mut claims = claims();
    claims.scope.clear();
    let authorization = bearer(&claims);

    // Act
    let result = verifier().authenticate(Some(&authorization), &runtime(), McpAuditBuffer::new());

    // Assert
    assert!(matches!(result, Err(AuthError::InvalidProvisioning)));
}

#[test]
fn should_reject_control_characters_in_bearer_subject() {
    // Arrange
    let mut claims = claims();
    claims.sub = "operator\n17".to_string();
    let authorization = bearer(&claims);

    // Act
    let result = verifier().authenticate(Some(&authorization), &runtime(), McpAuditBuffer::new());

    // Assert
    assert!(matches!(result, Err(AuthError::InvalidToken)));
}
