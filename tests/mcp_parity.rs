use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHasher,
};
use fitz::api::admin::auth::AdminPrincipal;
use fitz::api::admin::{QueueAgeBuckets, QueueInfo};
use fitz::api::http::Body;
use fitz::api::mcp::{McpCapabilityPolicy, McpExecutionContext, McpToolRegistry};
use fitz::auth::default_anonymous_permissions;
use fitz::boot::Runtime;
use fitz::runtime::Router;
use fitz::testkit::body;
use hyper::header::COOKIE;
use hyper::{Method, StatusCode};
use serde_json::Value;
use serial_test::serial;
use std::sync::Arc;

fn password_hash_for(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

fn configure_admin_auth() {
    std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
    std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
    std::env::set_var("FITZ_ADMIN_SESSION_TTL_SECS", "3600");
}

fn test_runtime() -> Arc<Runtime> {
    configure_admin_auth();
    let runtime = Runtime::new(Arc::new(Router::new()));
    runtime.mark_storage_ready();
    runtime.mark_domains_ready();
    runtime.mark_auth_config_ready();
    runtime.mark_startup_complete();
    Arc::new(runtime)
}

fn authenticated_cookie(runtime: &Arc<Runtime>) -> String {
    let principal = runtime
        .admin_auth()
        .authenticate_credentials("admin", "pwd123")
        .expect("admin principal");
    runtime
        .admin_auth()
        .issue_session_cookie(&principal)
        .expect("admin cookie")
}

fn authenticated_context() -> McpExecutionContext {
    McpExecutionContext::authenticated(
        AdminPrincipal {
            username: "admin".to_string(),
        },
        default_anonymous_permissions(),
    )
}

async fn rest_json(runtime: &Arc<Runtime>, path: &str) -> Value {
    let request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri(path)
        .header(COOKIE, authenticated_cookie(runtime))
        .body(Body::default())
        .expect("request");

    let response = fitz::api::admin::handlers::handle_request(request, runtime.clone())
        .await
        .expect("handler response");
    assert_eq!(response.status(), StatusCode::OK);

    serde_json::from_slice(
        &body::to_bytes(response.into_body())
            .await
            .expect("response body"),
    )
    .expect("json body")
}

fn normalize_uptime(value: &mut Value) {
    value["broker"]["uptime_seconds"] = Value::from(0);
}

#[tokio::test]
#[serial]
async fn should_mirror_rest_global_stats_via_mcp_tool() {
    // Arrange
    let runtime = test_runtime();
    let registry = McpToolRegistry::read_only();
    let policy = McpCapabilityPolicy::summary_only();

    // Act
    let mut rest_value = rest_json(&runtime, "/api/v1/stats").await;
    let mcp_value = registry
        .execute(
            "get_global_stats",
            &runtime,
            &authenticated_context(),
            &policy,
            None,
        )
        .expect("mcp stats output");

    // Assert
    normalize_uptime(&mut rest_value);
    let mut normalized_mcp_value = mcp_value.clone();
    normalize_uptime(&mut normalized_mcp_value);
    assert_eq!(normalized_mcp_value, rest_value);
}

#[tokio::test]
#[serial]
async fn should_mirror_rest_troubleshooting_via_mcp_tool() {
    // Arrange
    let runtime = test_runtime();
    let registry = McpToolRegistry::read_only();
    let policy = McpCapabilityPolicy::read_only();

    // Act
    let rest_value = rest_json(&runtime, "/api/v1/troubleshooting").await;
    let mcp_value = registry
        .execute(
            "explain_global_troubleshooting",
            &runtime,
            &authenticated_context(),
            &policy,
            None,
        )
        .expect("mcp troubleshooting output");

    // Assert
    assert_eq!(mcp_value, rest_value);
}

#[tokio::test]
#[serial]
async fn should_mirror_rest_resource_detail_via_mcp_tool() {
    // Arrange
    let runtime = test_runtime();
    let registry = McpToolRegistry::read_only();
    let policy = McpCapabilityPolicy::read_only();
    let arguments = serde_json::json!({
        "scheme": "kv",
        "realm": "acme",
        "area": "app",
        "resource": "users"
    });

    // Act
    let rest_value = rest_json(&runtime, "/api/v1/kv/realms/acme/areas/app/resources/users").await;
    let mcp_value = registry
        .execute(
            "inspect_resource_detail",
            &runtime,
            &authenticated_context(),
            &policy,
            Some(&arguments),
        )
        .expect("mcp resource detail output");

    // Assert
    assert_eq!(mcp_value, rest_value);
    assert_eq!(mcp_value["diagnostics"]["current_stage"], "healthy");
    let hints = mcp_value["diagnostics"]["explanation_hints"]
        .as_array()
        .expect("hints array");
    assert_eq!(hints.len(), 1);
    assert_eq!(hints[0], "No active pressure detected");
}

#[tokio::test]
#[serial]
async fn should_mirror_rest_resource_timeline_via_mcp_tool() {
    // Arrange
    let runtime = test_runtime();
    let registry = McpToolRegistry::read_only();
    let policy = McpCapabilityPolicy::read_only();
    let arguments = serde_json::json!({
        "scheme": "kv",
        "realm": "acme",
        "area": "app",
        "resource": "users",
        "limit": 5
    });

    // Act
    let rest_value = rest_json(
        &runtime,
        "/api/v1/kv/realms/acme/areas/app/resources/users/events?limit=5",
    )
    .await;
    let mcp_value = registry
        .execute(
            "inspect_resource_timeline",
            &runtime,
            &authenticated_context(),
            &policy,
            Some(&arguments),
        )
        .expect("mcp resource timeline output");

    // Assert
    assert_eq!(mcp_value, rest_value);
}

#[tokio::test]
#[serial]
async fn should_preserve_durable_backlog_label_given_queue_pressure() {
    // Arrange
    let runtime = test_runtime();
    let read_model = runtime.admin_read_model();
    read_model.replace_queues(vec![QueueInfo {
        family: 1,
        realm: "acme".to_string(),
        area: "app".to_string(),
        resource: "jobs".to_string(),
        messages_ready: 4,
        messages_delayed: 2,
        messages_inflight: 1,
        messages_dead_lettered: 0,
        messages_total: 7,
        oldest_message_age_seconds: 45,
        oldest_backlog_age_seconds: 45,
        backlog_age_buckets: QueueAgeBuckets::default(),
        delay_age_buckets: QueueAgeBuckets::default(),
    }]);
    let registry = McpToolRegistry::read_only();
    let policy = McpCapabilityPolicy::read_only();
    let arguments = serde_json::json!({
        "scheme": "queue",
        "realm": "acme",
        "area": "app",
        "resource": "jobs",
        "queue_family": 1
    });

    // Act
    let rest_value = rest_json(
        &runtime,
        "/api/v1/queue/realms/acme/areas/app/resources/jobs?family=1",
    )
    .await;
    let mcp_value = registry
        .execute(
            "inspect_resource_detail",
            &runtime,
            &authenticated_context(),
            &policy,
            Some(&arguments),
        )
        .expect("mcp queue detail output");

    // Assert
    assert_eq!(mcp_value, rest_value);
    assert_eq!(mcp_value["diagnostics"]["current_stage"], "backlog_growth");
    let hints = mcp_value["diagnostics"]["explanation_hints"]
        .as_array()
        .expect("hints array")
        .iter()
        .map(|value| value.as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    assert!(hints
        .iter()
        .any(|hint| hint.contains("Durable backlog with live processing lag")));
}
