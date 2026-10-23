use super::common::*;

/// Sets the fault-injection opt-in for one test and clears it on drop, so a
/// panicking assertion cannot leak it into the next serial test.
struct FailpointsEnabled;

impl FailpointsEnabled {
    fn new() -> Self {
        std::env::set_var("FITZ_DESTROYER_FAILPOINTS", "enabled");
        Self
    }
}

impl Drop for FailpointsEnabled {
    fn drop(&mut self) {
        std::env::remove_var("FITZ_DESTROYER_FAILPOINTS");
    }
}

fn failpoint_request(cookie: Option<String>, origin: &str) -> hyper::http::Request<Body> {
    failpoint_request_for("/destroyer/failpoints/notice-actor-panic", cookie, origin)
}

fn failpoint_request_for(
    uri: &str,
    cookie: Option<String>,
    origin: &str,
) -> hyper::http::Request<Body> {
    let mut builder = hyper::http::Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("host", "localhost")
        .header("origin", origin);
    if let Some(cookie) = cookie {
        builder = builder.header(COOKIE, cookie);
    }
    builder.body(Body::default()).unwrap()
}

#[tokio::test]
#[serial]
async fn should_hide_failpoint_routes_when_opt_in_is_unset() {
    // Arrange
    std::env::remove_var("FITZ_DESTROYER_FAILPOINTS");
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;
    let req = failpoint_request(Some(cookie), "http://localhost");

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn should_require_admin_session_for_failpoint_routes_when_enabled() {
    // Arrange
    let _enabled = FailpointsEnabled::new();
    let runtime = test_runtime();
    let req = failpoint_request(None, "http://localhost");

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial]
async fn should_reject_cross_origin_failpoint_requests_when_enabled() {
    // Arrange
    let _enabled = FailpointsEnabled::new();
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;
    let req = failpoint_request(Some(cookie), "http://evil.example");

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
#[serial]
async fn should_inject_failpoint_given_authenticated_same_origin_request() {
    // Arrange
    let _enabled = FailpointsEnabled::new();
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;
    let req = failpoint_request(Some(cookie), "http://localhost");

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["injected"], true);
    assert_eq!(payload["domain"], "notice");
}

#[tokio::test]
#[serial]
async fn should_require_admin_session_for_every_failpoint_route_when_enabled() {
    // Arrange
    let _enabled = FailpointsEnabled::new();
    let runtime = test_runtime();
    let routes = [
        "/destroyer/failpoints/kv-actor-panic",
        "/destroyer/failpoints/queue-actor-panic",
        "/destroyer/failpoints/lease-actor-panic",
        "/destroyer/failpoints/schedule-actor-panic",
        "/destroyer/failpoints/stream-actor-panic",
        "/destroyer/failpoints/rpc-actor-panic",
        "/destroyer/failpoints/all-domain-actor-panic",
        "/destroyer/failpoints/stream-family-1-actor-panic",
        "/destroyer/failpoints/rpc-family-1-actor-panic",
    ];

    // Act
    let mut statuses = Vec::new();
    for route in routes {
        let req = failpoint_request_for(route, None, "http://localhost");
        let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
            .await
            .unwrap();
        statuses.push((route, response.status()));
    }

    // Assert
    for (route, status) in statuses {
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{route}");
    }
}
