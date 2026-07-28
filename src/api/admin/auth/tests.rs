use super::*;
use crate::api::http::Body;
use hyper::header::COOKIE;
use serial_test::serial;

fn reset_admin_env() {
    for key in [
        "FITZ_ADMIN_AUTH_MODE",
        "FITZ_ROOT_PASSWORD",
        "FITZ_ADMIN_COOKIE_SECURE",
        "FITZ_ADMIN_PUBLIC_ORIGIN",
        "FITZ_ADMIN_ROUTE_FAMILIES",
    ] {
        std::env::remove_var(key);
    }
}

#[test]
#[serial]
fn should_authenticate_with_valid_credentials() {
    // Arrange
    reset_admin_env();
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");

    // Act
    let auth = AdminAuth::from_env();
    let principal = auth.authenticate_credentials("root", "pwd123");

    // Assert
    assert!(principal.is_ok());
}

#[test]
#[serial]
fn should_reject_a_legacy_configurable_admin_username() {
    // Arrange
    reset_admin_env();
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
    std::env::set_var("FITZ_ADMIN_USERNAME", "legacy-admin");

    // Act
    let auth = AdminAuth::from_env();
    let root = auth.authenticate_credentials("root", "pwd123");
    let legacy = auth.authenticate_credentials("legacy-admin", "pwd123");

    // Assert
    assert!(root.is_ok());
    assert!(matches!(legacy, Err(AuthFailure::InvalidCredentials)));
    std::env::remove_var("FITZ_ADMIN_USERNAME");
}

#[test]
#[serial]
fn should_rate_limit_admin_login_attempts_by_client_address() {
    // Arrange
    reset_admin_env();
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
    let auth = AdminAuth::from_env();
    let client = AdminClientIp("192.0.2.10".parse().unwrap());

    // Act
    let accepted = (0..ADMIN_LOGIN_ATTEMPT_LIMIT)
        .map(|_| auth.begin_login_attempt(client))
        .collect::<Vec<_>>();
    let rejected = auth.begin_login_attempt(client);

    // Assert
    assert!(accepted.iter().all(Result::is_ok));
    assert!(matches!(rejected, Err(AuthFailure::RateLimited)));
}

#[test]
#[serial]
fn should_configure_protected_admin_without_jwt_secret() {
    // Arrange
    reset_admin_env();
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
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
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
    std::env::remove_var("FITZ_ADMIN_JWT_SECRET");

    let issuing_auth = AdminAuth::from_env();
    let principal = issuing_auth
        .authenticate_credentials("root", "pwd123")
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
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");

    let auth = AdminAuth::from_env();
    let principal = auth.authenticate_credentials("root", "pwd123").unwrap();
    let cookie = auth.issue_session_cookie(&principal).unwrap();
    let cookie_value = cookie.split(';').next().unwrap().to_string();

    let req = hyper::http::Request::builder()
        .header(COOKIE, cookie_value)
        .body(Body::default())
        .unwrap();

    // Act
    let extracted = auth.principal_from_request(&req).unwrap();

    // Assert
    assert_eq!(extracted.username, "root");
    assert!(extracted.route_family_access.is_wildcard());
}

#[test]
#[serial]
fn should_reject_symbolic_route_family_grants_at_login() {
    // Arrange
    reset_admin_env();
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
    std::env::set_var("FITZ_ADMIN_ROUTE_FAMILIES", "1,partner");

    let auth = AdminAuth::from_env();
    // Act
    let result = auth.authenticate_credentials("root", "pwd123");

    // Assert
    assert!(matches!(result, Err(AuthFailure::Unavailable)));
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
    assert_eq!(principal.username, "root");
    assert!(principal.route_family_access.is_wildcard());
}

#[test]
#[serial]
fn should_mark_admin_session_cookie_secure_by_default() {
    // Arrange
    reset_admin_env();
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
    let auth = AdminAuth::from_env();
    let principal = auth.authenticate_credentials("root", "pwd123").unwrap();

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
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
    std::env::set_var("FITZ_ADMIN_COOKIE_SECURE", "false");
    let auth = AdminAuth::from_env();
    let principal = auth.authenticate_credentials("root", "pwd123").unwrap();

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
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
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

#[tokio::test]
async fn should_reject_login_body_over_limit() {
    // Arrange
    let oversized = "a".repeat(8 * 1024 + 1);
    let request = hyper::Request::new(Body::from(oversized));

    // Act
    let result = parse_login_request(request).await;

    // Assert
    assert!(matches!(result, Err(AuthFailure::InvalidCredentials)));
}

#[test]
#[serial]
fn should_validate_same_origin_for_protected_admin_request() {
    // Arrange
    reset_admin_env();
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
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
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
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
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
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
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
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
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
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
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
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
