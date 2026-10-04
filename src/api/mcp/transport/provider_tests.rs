//! Opt-in acceptance against a real externally started OAuth provider.

use super::*;
use rmcp::model::{
    CallToolRequestParams, ClientConfig, ProtocolVersion, ReadResourceRequestParams,
};
use rmcp::service::{ClientLifecycleMode, ClientServiceExt};
use rmcp::transport::child_process::TokioChildProcess;
use rmcp::transport::StreamableHttpClientTransport;

#[test]
fn should_reject_old_signatures_after_installing_a_rotated_signing_pin() {
    // Arrange
    let state = tests::http_state_for_tests();
    let authorization =
        hyper::header::HeaderValue::from_str(&format!("Bearer {}", tests::protocol_test_token()))
            .unwrap();
    let rotated = oauth::McpOAuthVerifier::new(
        "https://identity.example.test".into(),
        "fitz-tests".into(),
        "https://fitz.example.test/mcp",
        include_bytes!("testdata/insecure_test_only_rotated_rsa_public.pem"),
        None,
    )
    .expect("valid replacement RSA signing pin");

    // Act
    let current_result =
        state
            .verifier
            .authenticate(Some(&authorization), &state.runtime, McpAuditBuffer::new());
    let rotated_result =
        rotated.authenticate(Some(&authorization), &state.runtime, McpAuditBuffer::new());

    // Assert
    assert!(current_result.is_ok());
    assert!(matches!(
        rotated_result,
        Err(oauth::AuthError::InvalidToken)
    ));
}

#[tokio::test]
#[ignore = "requires FITZ_MCP_PROVIDER_TOKEN_FILE and FITZ_MCP_PROVIDER_PUBLIC_KEY_FILE from the pinned provider fixture"]
async fn should_authorize_real_provider_machine_grant_through_production_http_wrapper() {
    // Arrange
    let token_file = std::env::var("FITZ_MCP_PROVIDER_TOKEN_FILE").expect("provider token file");
    let key_file =
        std::env::var("FITZ_MCP_PROVIDER_PUBLIC_KEY_FILE").expect("provider public key file");
    let token = std::fs::read_to_string(token_file).expect("read protected provider token");
    let key = std::fs::read(key_file).expect("read provider public key");
    let mut state = tests::http_state_for_tests();
    state.verifier = Arc::new(
        oauth::McpOAuthVerifier::new(
            "https://identity.example.test/realms/fitz-mcp-example".into(),
            "https://fitz.example.test/mcp".into(),
            "https://fitz.example.test/mcp",
            &key,
            None,
        )
        .expect("real provider signing pin"),
    );
    let (url, stop, server) = tests::spawn_http_state(state).await;
    let transport = StreamableHttpClientTransport::from_config(
        tests::authenticated_transport_config(url, token.trim().into()),
    );
    let client = ClientConfig::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .expect("provider-authenticated primary MCP client");
    let scope = serde_json::json!({"scheme":"queue", "route_family":1, "realm":"example-realm", "area":"operations", "resource":"jobs"});

    // Act
    let allowed = client
        .call_tool(
            CallToolRequestParams::new("inspect_resource_detail")
                .with_arguments(serde_json::from_value(scope.clone()).unwrap()),
        )
        .await
        .expect("authorized scoped read");
    let mut forbidden_scope = scope;
    forbidden_scope["realm"] = "other-realm".into();
    let denied = client
        .call_tool(
            CallToolRequestParams::new("inspect_resource_detail")
                .with_arguments(serde_json::from_value(forbidden_scope).unwrap()),
        )
        .await
        .expect("permission denial response");

    // Assert
    assert_ne!(allowed.is_error, Some(true));
    assert_eq!(denied.is_error, Some(true));
    client.cancel().await.expect("close provider client");
    stop.cancel();
    server.await.expect("provider fixture HTTP listener");
}

#[tokio::test]
#[ignore = "requires FITZ_MCP_STDIO_BIN for the compiled process adapter"]
async fn should_forward_stdio_process_to_production_bearer_endpoint_and_restrict_resources() {
    // Arrange
    let binary = std::env::var("FITZ_MCP_STDIO_BIN").expect("compiled stdio adapter");
    let token_file = tempfile::NamedTempFile::new().expect("protected token file");
    std::fs::write(token_file.path(), tests::protocol_test_token())
        .expect("write signed test bearer");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(token_file.path(), std::fs::Permissions::from_mode(0o600))
            .unwrap();
    }
    let (url, stop, server) = tests::spawn_loopback_http_state(tests::http_state_for_tests()).await;
    let mut command = tokio::process::Command::new(binary);
    command
        .env("FITZ_MCP_HTTP_URL", url)
        .env("FITZ_MCP_BEARER_TOKEN_FILE", token_file.path())
        .stderr(std::process::Stdio::null());
    let transport = TokioChildProcess::new(command).expect("spawn real stdio adapter");
    let client = ClientConfig::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .expect("stdio client connected to production bearer endpoint");

    // Act
    let tools = client
        .list_tools(None)
        .await
        .expect("forwarded real tool catalog");
    let allowed = client
        .read_resource(ReadResourceRequestParams::new(
            "fitz://resource/v1/kv/1/customer-space/jobs/dispatch",
        ))
        .await;
    let denied = client
        .read_resource(ReadResourceRequestParams::new(
            "fitz://resource/v1/kv/1/other-space/jobs/dispatch",
        ))
        .await;

    // Assert
    assert!(tools
        .tools
        .iter()
        .any(|tool| tool.name == "inspect_resource_detail"));
    assert!(allowed.is_ok());
    assert!(denied.is_err());
    client.cancel().await.expect("close real stdio client");
    stop.cancel();
    server.await.expect("production wrapper HTTP listener");
}
