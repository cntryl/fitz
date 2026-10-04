use super::*;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};

fn request(mut body: serde_json::Value, revision: &str, method: &str) -> Request<Full<Bytes>> {
    if revision == "2026-07-28" {
        body["params"]["_meta"] = serde_json::json!({
            "io.modelcontextprotocol/protocolVersion": revision,
            "io.modelcontextprotocol/clientCapabilities": {}
        });
    }
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(HOST, "fitz.example.test")
        .header(
            AUTHORIZATION,
            format!("Bearer {}", tests::protocol_test_token()),
        )
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(hyper::header::ACCEPT, "application/json, text/event-stream")
        .header("mcp-protocol-version", revision)
        .header("mcp-method", method);
    if revision == "2026-07-28" {
        if let Some(name) = body["params"]
            .get("name")
            .or_else(|| body["params"].get("uri"))
            .and_then(serde_json::Value::as_str)
        {
            builder = builder.header("mcp-name", name);
        }
    }
    builder
        .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
        .unwrap()
}

#[tokio::test]
async fn should_require_authentication_and_audit_without_recording_credentials() {
    // Arrange
    let state = tests::http_state_for_tests();
    let request = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(HOST, "fitz.example.test")
        .header(hyper::header::COOKIE, "admin_session=secret-browser-cookie")
        .body(Full::new(Bytes::new()))
        .unwrap();

    // Act
    let response = state.handle(request).await;
    let records = state.audit_buffer.records();

    // Assert
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(response.headers().contains_key(WWW_AUTHENTICATE));
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].principal, None);
    assert_eq!(records[0].result_summary, "invalid_request");
    assert!(!serde_json::to_string(&records)
        .unwrap()
        .contains("secret-browser-cookie"));
}

#[tokio::test]
async fn should_reject_http_admission_when_full_before_collecting_body() {
    // Arrange
    let state = tests::http_state_for_tests();
    let permits = state
        .request_slots
        .clone()
        .try_acquire_many_owned(u32::try_from(MAX_HTTP_CONCURRENCY).unwrap())
        .unwrap();
    let request = request(
        serde_json::json!({"jsonrpc":"2.0","id":7,"method":"tools/list"}),
        "2026-07-28",
        "tools/list",
    );

    // Act
    let response = state.handle(request).await;

    // Assert
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        state.audit_buffer.records()[0].result_summary,
        "http_capacity_full"
    );
    drop(permits);
}

#[tokio::test]
async fn should_reject_unsupported_protocol_revision_before_dispatch() {
    // Arrange
    let state = tests::http_state_for_tests();
    let request = request(
        serde_json::json!({"jsonrpc":"2.0","id":7,"method":"tools/list"}),
        "2099-01-01",
        "tools/list",
    );

    // Act
    let response = state.handle(request).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        state.audit_buffer.records()[0].result_summary,
        "protocol_rejected"
    );
}

#[tokio::test]
async fn should_reject_header_body_mismatch_before_dispatch() {
    // Arrange
    let state = tests::http_state_for_tests();
    let request = request(
        serde_json::json!({"jsonrpc":"2.0","id":7,"method":"tools/list"}),
        "2026-07-28",
        "resources/list",
    );

    // Act
    let response = state.handle(request).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        state.audit_buffer.records()[0].result_summary,
        "protocol_rejected"
    );
}

#[tokio::test]
async fn should_preserve_request_id_in_actual_encoded_catalog_response() {
    // Arrange
    let state = tests::http_state_for_tests();
    let id = "client/request-17";
    let request = request(
        serde_json::json!({"jsonrpc":"2.0","id":id,"method":"tools/list"}),
        "2026-07-28",
        "tools/list",
    );

    // Act
    let response = state.handle(request).await;
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    // Assert
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], id);
    assert!(body["result"]["tools"].is_array());
    assert!(bytes.len() <= limits::MAX_RESPONSE_BYTES);
}

#[tokio::test]
async fn should_omit_disabled_action_tools_from_http_catalog() {
    // Arrange
    let state = tests::http_state_for_tests();
    let request = request(
        serde_json::json!({"jsonrpc":"2.0","id":7,"method":"tools/list"}),
        "2026-07-28",
        "tools/list",
    );

    // Act
    let response = state.handle(request).await;
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    // Assert
    let tools = body["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 10);
    assert!(tools
        .iter()
        .all(|tool| !actions::McpActionState::is_tool(tool["name"].as_str().unwrap())));
}

#[tokio::test]
async fn should_reject_direct_call_to_disabled_action_tool() {
    // Arrange
    let state = tests::http_state_for_tests();
    let request = request(
        serde_json::json!({"jsonrpc":"2.0","id":7,"method":"tools/call",
            "params":{"name":actions::CONFIRM_DRAIN_TOOL,"arguments":{}}}),
        "2026-07-28",
        "tools/call",
    );

    // Act
    let response = state.handle(request).await;
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    // Assert
    assert!(
        body.get("error").is_some() || body["result"]["isError"] == true,
        "{body}"
    );
    assert_eq!(state.runtime.lifecycle_state().as_str(), "running");
    assert!(state
        .audit_buffer
        .records()
        .iter()
        .any(|record| record.result_summary == "unknown_tool"));
}

#[tokio::test]
async fn should_audit_resource_validation_error_and_preserve_request_id() {
    // Arrange
    let state = tests::http_state_for_tests();
    let request = request(
        serde_json::json!({"jsonrpc":"2.0","id":7,"method":"resources/read",
            "params":{"uri":"fitz://resource/v99/kv/1/customer-space/jobs/dispatch"}}),
        "2026-07-28",
        "resources/read",
    );

    // Act
    let response = state.handle(request).await;
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    // Assert
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["id"], 7);
    assert!(body.get("error").is_some(), "{body}");
    assert!(state
        .audit_buffer
        .records()
        .iter()
        .any(|record| record.result_summary == "protocol_rejected"));
}

#[tokio::test]
async fn should_close_expired_compatibility_session_before_releasing_binding() {
    // Arrange
    let state = tests::http_state_for_tests();
    let initialize = request(
        serde_json::json!({
            "jsonrpc":"2.0", "id":7, "method":"initialize",
            "params":{"protocolVersion":"2025-11-25", "capabilities":{},
                "clientInfo":{"name":"expiry-test","version":"1"}}
        }),
        "2025-11-25",
        "initialize",
    );
    let initialized = state.handle(initialize).await;
    assert_eq!(initialized.status(), StatusCode::OK);
    let session_id = initialized
        .headers()
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    state
        .sessions
        .lock()
        .get_mut(&session_id)
        .unwrap()
        .expires_at = SystemTime::UNIX_EPOCH;

    // Act
    state.reap_expired_sessions().await;
    let mut probe = request(
        serde_json::json!({"jsonrpc":"2.0","id":8,"method":"tools/list"}),
        "2025-11-25",
        "tools/list",
    );
    probe
        .headers_mut()
        .insert("mcp-session-id", session_id.parse().unwrap());
    let response = state.inner.clone().handle(probe).await;

    // Assert
    assert!(state.sessions.lock().is_empty());
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn should_keep_session_binding_when_transport_delete_is_rejected() {
    // Arrange
    let state = tests::http_state_for_tests();
    let initialize = request(
        serde_json::json!({
            "jsonrpc":"2.0", "id":7, "method":"initialize",
            "params":{"protocolVersion":"2025-11-25", "capabilities":{},
                "clientInfo":{"name":"delete-test","version":"1"}}
        }),
        "2025-11-25",
        "initialize",
    );
    let initialized = state.handle(initialize).await;
    let session_id = initialized
        .headers()
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let deletion = |revision| {
        Request::builder()
            .method(Method::DELETE)
            .uri("/mcp")
            .header(HOST, "fitz.example.test")
            .header(
                AUTHORIZATION,
                format!("Bearer {}", tests::protocol_test_token()),
            )
            .header("mcp-protocol-version", revision)
            .header("mcp-session-id", session_id.as_str())
            .body(Full::new(Bytes::new()))
            .unwrap()
    };

    // Act
    let rejected = state.handle(deletion("2026-07-28")).await;
    let binding_retained = state.sessions.lock().contains_key(&session_id);
    let accepted = state.handle(deletion("2025-11-25")).await;

    // Assert
    assert_eq!(rejected.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert!(binding_retained);
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    assert!(state.sessions.lock().is_empty());
}
