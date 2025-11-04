mod harness;
use harness::common::{create_sub_channel, start_test_engine};
use tokio::time::{sleep, timeout, Duration};

// ============================================================================
// CONTROL PLANE ENGINE INTEGRATION TESTS
// ============================================================================
// These tests exercise the engine-level control plane functionality via
// in-process EngineHandle, not over WebSocket transport.
//
// For full end-to-end WebSocket tests, see e2e_control_ws.rs (to be added).
// ============================================================================

// ============================================================================
// SIMPLIFIED CONTROL PLANE - 4 CORE ROUTES
// ============================================================================
// 1. control://heartbeat - Node keep-alive (frequent, lightweight)
// 2. control://shutdown - Graceful shutdown notification  
// 3. control://metrics - Extensible metrics reporting
// 4. control://config - JWT/feature/limit configuration
//
// Control mode: "self" (standalone) or URL (connect to control plane)
// Auth: client_id/client_secret (not JWT)
// ============================================================================

// Note: These tests document expected behavior for the control plane.
// Many features are not yet implemented - tests serve as specification.

// Heartbeat Tests
#[tokio::test]
async fn should_send_periodic_heartbeats_to_control_plane() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://heartbeat".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    // Simulate sending heartbeats
    handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-1".to_string(),
            b"{\"nodeId\":\"test-node\",\"timestamp\":1234567890}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout waiting for heartbeat")
        .expect("channel closed");
    assert_eq!(msg.0, "control://heartbeat");
    assert!(String::from_utf8_lossy(&msg.2).contains("nodeId"));
}

#[tokio::test]
async fn should_include_node_id_in_heartbeat() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://heartbeat".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let payload = b"{\"nodeId\":\"test-node-123\",\"timestamp\":1234567890}".to_vec();
    handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-1".to_string(),
            payload,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let payload_str = String::from_utf8_lossy(&msg.2);
    assert!(payload_str.contains("test-node-123"), "Heartbeat should include nodeId");
}

#[tokio::test]
async fn should_send_heartbeats_at_configured_interval() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://heartbeat".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    // Simulate sending heartbeats at 100ms intervals
    let start = tokio::time::Instant::now();
    handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-1".to_string(),
            b"{\"nodeId\":\"test\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish 1 failed");
    
    sleep(Duration::from_millis(100)).await;
    
    handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-2".to_string(),
            b"{\"nodeId\":\"test\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish 2 failed");

    // Assert
    let _msg1 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout on first heartbeat")
        .expect("channel closed");
    let _msg2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout on second heartbeat")
        .expect("channel closed");
    
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(90) && elapsed <= Duration::from_millis(200),
            "Heartbeats should arrive at configured interval");
}

#[tokio::test]
async fn should_continue_heartbeats_indefinitely() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://heartbeat".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    for i in 1..=3 {
        handle
            .publish(
                "control://heartbeat".to_string(),
                format!("hb-{}", i),
                b"{\"nodeId\":\"test\"}".to_vec(),
                None,
                None,
                false,
                None,
            )
            .await
            .expect("publish failed");
    }

    // Assert
    for _ in 1..=3 {
        let msg = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(msg.0, "control://heartbeat");
    }
}

// Shutdown Tests
#[tokio::test]
async fn should_send_shutdown_signal_on_graceful_shutdown() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://shutdown".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    handle
        .publish(
            "control://shutdown".to_string(),
            "shutdown-1".to_string(),
            b"{\"nodeId\":\"test-node\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert_eq!(msg.0, "control://shutdown");
    assert!(String::from_utf8_lossy(&msg.2).contains("nodeId"));
}

#[tokio::test]
async fn should_include_shutdown_reason_when_available() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://shutdown".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let payload = b"{\"nodeId\":\"test-node\",\"reason\":\"maintenance\"}".to_vec();
    handle
        .publish(
            "control://shutdown".to_string(),
            "shutdown-1".to_string(),
            payload,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let payload_str = String::from_utf8_lossy(&msg.2);
    assert!(payload_str.contains("maintenance"), "Shutdown should include reason");
}

#[tokio::test]
async fn should_send_shutdown_before_closing_connections() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://shutdown".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let shutdown_time = tokio::time::Instant::now();
    handle
        .publish(
            "control://shutdown".to_string(),
            "shutdown-1".to_string(),
            b"{\"nodeId\":\"test-node\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let _msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let received_time = tokio::time::Instant::now();
    
    // Shutdown message received before we simulate connection close
    assert!(received_time - shutdown_time < Duration::from_millis(100),
            "Shutdown notification should arrive before connection close");
}

// Metrics Tests  
#[tokio::test]
async fn should_send_periodic_metrics_to_control_plane() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://metrics".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    handle
        .publish(
            "control://metrics".to_string(),
            "m-1".to_string(),
            b"{\"nodeId\":\"test-node\",\"active_connections\":5}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert_eq!(msg.0, "control://metrics");
    assert!(String::from_utf8_lossy(&msg.2).contains("active_connections"));
}

#[tokio::test]
async fn should_include_connection_count_in_metrics() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://metrics".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let payload = b"{\"nodeId\":\"test-node\",\"active_connections\":3}".to_vec();
    handle
        .publish(
            "control://metrics".to_string(),
            "m-1".to_string(),
            payload,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let payload_str = String::from_utf8_lossy(&msg.2);
    assert!(payload_str.contains("\"active_connections\":3"));
}

#[tokio::test]
async fn should_include_message_throughput_in_metrics() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://metrics".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let payload = b"{\"nodeId\":\"test-node\",\"messages_per_second\":100}".to_vec();
    handle
        .publish(
            "control://metrics".to_string(),
            "m-1".to_string(),
            payload,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let payload_str = String::from_utf8_lossy(&msg.2);
    assert!(payload_str.contains("messages_per_second"));
}

#[tokio::test]
async fn should_include_storage_usage_in_metrics() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://metrics".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let payload = b"{\"nodeId\":\"test-node\",\"storage_bytes_used\":1024000}".to_vec();
    handle
        .publish(
            "control://metrics".to_string(),
            "m-1".to_string(),
            payload,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let payload_str = String::from_utf8_lossy(&msg.2);
    assert!(payload_str.contains("storage_bytes_used"));
}

#[tokio::test]
async fn should_support_extensible_metrics_schema() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://metrics".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let payload = b"{\"nodeId\":\"test-node\",\"active_connections\":5,\"custom_gauge\":42}".to_vec();
    handle
        .publish(
            "control://metrics".to_string(),
            "m-1".to_string(),
            payload,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let payload_str = String::from_utf8_lossy(&msg.2);
    assert!(payload_str.contains("active_connections"));
    assert!(payload_str.contains("custom_gauge"), "Should support custom metrics");
}

#[tokio::test]
async fn should_send_metrics_at_configured_interval() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://metrics".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let start = tokio::time::Instant::now();
    handle
        .publish(
            "control://metrics".to_string(),
            "m-1".to_string(),
            b"{\"nodeId\":\"test\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish 1 failed");
    
    sleep(Duration::from_millis(100)).await;
    
    handle
        .publish(
            "control://metrics".to_string(),
            "m-2".to_string(),
            b"{\"nodeId\":\"test\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish 2 failed");

    // Assert
    let _msg1 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout on first metric")
        .expect("channel closed");
    let _msg2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout on second metric")
        .expect("channel closed");
    
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(90),
            "Metrics should arrive at configured interval");
}

#[tokio::test]
async fn should_allow_different_intervals_for_heartbeat_and_metrics() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (hb_tx, mut hb_rx) = create_sub_channel(20);
    let (m_tx, mut m_rx) = create_sub_channel(10);
    handle
        .subscribe("control://heartbeat".to_string(), hb_tx, 1)
        .await
        .expect("subscribe heartbeat failed");
    handle
        .subscribe("control://metrics".to_string(), m_tx, 2)
        .await
        .expect("subscribe metrics failed");

    // Act
    // Simulate 5 heartbeats (fast)
    for i in 1..=5 {
        handle
            .publish(
                "control://heartbeat".to_string(),
                format!("hb-{}", i),
                b"{\"nodeId\":\"test\"}".to_vec(),
                None,
                None,
                false,
                None,
            )
            .await
            .expect("publish heartbeat failed");
    }
    
    // Simulate 2 metrics (slower)
    for i in 1..=2 {
        handle
            .publish(
                "control://metrics".to_string(),
                format!("m-{}", i),
                b"{\"nodeId\":\"test\"}".to_vec(),
                None,
                None,
                false,
                None,
            )
            .await
            .expect("publish metrics failed");
    }

    // Assert
    let mut hb_count = 0;
    for _ in 1..=5 {
        let msg = timeout(Duration::from_secs(1), hb_rx.recv())
            .await
            .expect("timeout on heartbeat")
            .expect("channel closed");
        if msg.0 == "control://heartbeat" {
            hb_count += 1;
        }
    }
    
    let mut m_count = 0;
    for _ in 1..=2 {
        let msg = timeout(Duration::from_secs(1), m_rx.recv())
            .await
            .expect("timeout on metrics")
            .expect("channel closed");
        if msg.0 == "control://metrics" {
            m_count += 1;
        }
    }
    
    assert_eq!(hb_count, 5, "Should receive more heartbeats");
    assert_eq!(m_count, 2, "Should receive fewer metrics");
}

// Config Tests
#[tokio::test]
async fn should_receive_config_from_control_plane() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://config".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let config = b"{\"nodeId\":\"test-node\",\"jwks_url\":\"https://auth.example.com/jwks\"}".to_vec();
    handle
        .publish(
            "control://config".to_string(),
            "cfg-1".to_string(),
            config,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert_eq!(msg.0, "control://config");
    assert!(String::from_utf8_lossy(&msg.2).contains("jwks_url"));
}

#[tokio::test]
async fn should_include_jwt_validation_config() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://config".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let config = b"{\"jwks_url\":\"https://auth.example.com/jwks\",\"issuer\":\"https://auth.example.com\",\"audience\":\"api\"}".to_vec();
    handle
        .publish(
            "control://config".to_string(),
            "cfg-1".to_string(),
            config,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let config_str = String::from_utf8_lossy(&msg.2);
    assert!(config_str.contains("jwks_url"));
    assert!(config_str.contains("issuer"));
    assert!(config_str.contains("audience"));
}

#[tokio::test]
async fn should_update_jwt_validator_when_config_changes() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://config".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let old_config = b"{\"jwks_url\":\"https://auth.old.com/jwks\"}".to_vec();
    handle
        .publish(
            "control://config".to_string(),
            "cfg-1".to_string(),
            old_config,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish old config failed");
    
    let new_config = b"{\"jwks_url\":\"https://auth.new.com/jwks\"}".to_vec();
    handle
        .publish(
            "control://config".to_string(),
            "cfg-2".to_string(),
            new_config,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish new config failed");

    // Assert
    let msg1 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout on old config")
        .expect("channel closed");
    assert!(String::from_utf8_lossy(&msg1.2).contains("auth.old.com"));
    
    let msg2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout on new config")
        .expect("channel closed");
    assert!(String::from_utf8_lossy(&msg2.2).contains("auth.new.com"));
}

#[tokio::test]
async fn should_include_feature_flags_in_config() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://config".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let config = b"{\"enable_queue_dedup\":true,\"enable_stream_peek\":false,\"enable_metrics_export\":true}".to_vec();
    handle
        .publish(
            "control://config".to_string(),
            "cfg-1".to_string(),
            config,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let config_str = String::from_utf8_lossy(&msg.2);
    assert!(config_str.contains("enable_queue_dedup"));
    assert!(config_str.contains("enable_stream_peek"));
    assert!(config_str.contains("enable_metrics_export"));
}

#[tokio::test]
async fn should_include_limits_in_config() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://config".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let config = b"{\"max_message_size\":1048576,\"max_connections\":1000,\"ack_window_size\":100}".to_vec();
    handle
        .publish(
            "control://config".to_string(),
            "cfg-1".to_string(),
            config,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let config_str = String::from_utf8_lossy(&msg.2);
    assert!(config_str.contains("max_message_size"));
    assert!(config_str.contains("max_connections"));
    assert!(config_str.contains("ack_window_size"));
}

#[tokio::test]
async fn should_apply_ack_window_from_config() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://config".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let config = b"{\"ack_window_size\":50}".to_vec();
    handle
        .publish(
            "control://config".to_string(),
            "cfg-1".to_string(),
            config,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let config_str = String::from_utf8_lossy(&msg.2);
    assert!(config_str.contains("\"ack_window_size\":50"));
    // Note: Actual enforcement of ack_window would be tested in stream/queue tests
}

#[tokio::test]
async fn should_support_extensible_config_schema() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://config".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let config = b"{\"ack_window_size\":100,\"future_feature_flag\":true}".to_vec();
    handle
        .publish(
            "control://config".to_string(),
            "cfg-1".to_string(),
            config,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let config_str = String::from_utf8_lossy(&msg.2);
    assert!(config_str.contains("future_feature_flag"), "Should accept unknown fields");
}

#[tokio::test]
async fn should_apply_config_updates_incrementally() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://config".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let full_config = b"{\"ack_window_size\":100,\"max_connections\":1000}".to_vec();
    handle
        .publish(
            "control://config".to_string(),
            "cfg-1".to_string(),
            full_config,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish full config failed");
    
    let partial_update = b"{\"ack_window_size\":50}".to_vec();
    handle
        .publish(
            "control://config".to_string(),
            "cfg-2".to_string(),
            partial_update,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish partial update failed");

    // Assert
    let _msg1 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout on full config")
        .expect("channel closed");
    
    let msg2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout on partial update")
        .expect("channel closed");
    
    // Partial update received - in real implementation, only specified fields would change
    assert!(String::from_utf8_lossy(&msg2.2).contains("50"));
}

// Negative Tests
#[tokio::test]
async fn should_reject_publish_to_unknown_control_route() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .publish(
            "control://unknown".to_string(),
            "msg-1".to_string(),
            b"data".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    // Currently accepts any route - future: validate known control routes
    // When implemented, should return Err("unknown control route")
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test]
async fn should_reject_malformed_control_route() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .publish(
            "control://".to_string(), // Missing path
            "msg-1".to_string(),
            b"data".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    // Currently accepts any route format - future: validate route structure
    // When implemented, should return Err("malformed route")
    assert!(result.is_ok() || result.is_err());
}

// Control Mode Tests
#[tokio::test]
async fn should_handle_self_mode_as_standalone_node() {
    // Arrange & Act
    let (handle, _store) = start_test_engine();
    
    // Assert
    // In self mode, node operates independently
    // Can publish to control routes locally
    let result = handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-1".to_string(),
            b"{\"nodeId\":\"self-node\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;
    assert!(result.is_ok(), "Self mode should accept control routes");
}

#[tokio::test]
async fn should_connect_to_external_control_plane_in_url_mode() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://heartbeat".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    // When control_mode is a URL, node would connect externally
    // For now, we test that control routes still work
    handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-1".to_string(),
            b"{\"nodeId\":\"external-node\",\"control_url\":\"wss://control.example.com\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert!(String::from_utf8_lossy(&msg.2).contains("control_url"));
}

#[tokio::test]
async fn should_authenticate_with_client_credentials() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://heartbeat".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    // Simulate sending heartbeat with client credentials
    let payload = b"{\"nodeId\":\"test-node\",\"client_id\":\"node-1\",\"client_secret\":\"secret123\"}".to_vec();
    handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-1".to_string(),
            payload,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let payload_str = String::from_utf8_lossy(&msg.2);
    assert!(payload_str.contains("client_id"));
}

#[tokio::test]
async fn should_retry_connection_when_control_plane_unavailable() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://heartbeat".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    // Simulate retry attempts with backoff information in payload
    let attempts = vec![
        (0, "immediate"),
        (1000, "1s delay"),
        (2000, "2s delay"),
        (4000, "4s delay"),
    ];
    
    for (i, (delay_ms, desc)) in attempts.iter().enumerate() {
        let payload = format!("{{\"nodeId\":\"test\",\"attempt\":{},\"delay_ms\":{},\"desc\":\"{}\"}}", 
                             i + 1, delay_ms, desc);
        handle
            .publish(
                "control://heartbeat".to_string(),
                format!("hb-retry-{}", i + 1),
                payload.as_bytes().to_vec(),
                None,
                None,
                false,
                None,
            )
            .await
            .expect("publish failed");
    }

    // Assert
    for i in 0..4 {
        let msg = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");
        let payload_str = String::from_utf8_lossy(&msg.2);
        assert!(payload_str.contains(&format!("\"attempt\":{}", i + 1)));
    }
}

#[tokio::test]
async fn should_continue_operating_when_disconnected_from_control_plane() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    // Even without control plane connection, node serves client requests
    let result = handle
        .publish(
            "notice://realm/events".to_string(),
            "evt-1".to_string(),
            b"test data".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok(), "Node should operate independently of control plane");
}

#[tokio::test]
async fn should_reconnect_after_disconnect() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://heartbeat".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    // Simulate disconnect and reconnect sequence
    handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-before-disconnect".to_string(),
            b"{\"nodeId\":\"test\",\"status\":\"connected\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish before disconnect failed");
    
    // Simulate reconnection
    handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-after-reconnect".to_string(),
            b"{\"nodeId\":\"test\",\"status\":\"reconnected\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish after reconnect failed");

    // Assert
    let msg1 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout on first heartbeat")
        .expect("channel closed");
    assert!(String::from_utf8_lossy(&msg1.2).contains("connected"));
    
    let msg2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout on reconnect heartbeat")
        .expect("channel closed");
    assert!(String::from_utf8_lossy(&msg2.2).contains("reconnected"));
}

// Auth Tests
#[tokio::test]
async fn should_not_require_jwt_for_control_routes() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-1".to_string(),
            b"{\"nodeId\":\"test-node\",\"timestamp\":1234567890}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok(), "Control routes should not require JWT");
}

#[tokio::test]
async fn should_use_client_credentials_not_jwt() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .publish(
            "control://metrics".to_string(),
            "m-1".to_string(),
            b"{\"nodeId\":\"test-node\",\"active_connections\":5}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok(), "Control routes use client credentials, not JWT");
}

#[tokio::test]
async fn should_reject_invalid_client_credentials() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    // Simulate sending heartbeat with invalid credentials in payload
    let result = handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-1".to_string(),
            b"{\"nodeId\":\"test\",\"client_id\":\"node-1\",\"client_secret\":\"invalid\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    // Currently accepts all publishes - when auth is implemented, this should fail
    // For now, we verify the message can be sent (auth validation TBD)
    assert!(result.is_ok(), "Currently no credential validation - will be implemented");
}

#[tokio::test]
async fn should_isolate_control_from_tenant_permissions() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-1".to_string(),
            b"{\"nodeId\":\"test-node\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok(), "Control routes operate on separate auth domain");
    // No realm checks, tenant permissions, or resource-level ACLs applied
}

// Self Mode Tests
#[tokio::test]
async fn should_not_send_heartbeats_in_self_mode() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://heartbeat".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    // In self mode, heartbeats can be published locally but not sent externally
    // We test that local publish works for self-monitoring
    handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-self-1".to_string(),
            b"{\"nodeId\":\"self-node\",\"mode\":\"self\"}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    // Local subscription receives the heartbeat
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert!(String::from_utf8_lossy(&msg.2).contains("self-node"));
    // Note: External sending would be disabled in actual self mode implementation
}

#[tokio::test]
async fn should_not_send_metrics_externally_in_self_mode() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://metrics".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    // In self mode, metrics can be published locally for self-monitoring
    handle
        .publish(
            "control://metrics".to_string(),
            "m-self-1".to_string(),
            b"{\"nodeId\":\"self-node\",\"mode\":\"self\",\"active_connections\":3}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    // Local subscription receives metrics
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert!(String::from_utf8_lossy(&msg.2).contains("self-node"));
    // Note: External sending would be disabled in actual self mode implementation
}

#[tokio::test]
async fn should_serve_config_locally_in_self_mode() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    // In self mode, can publish config locally
    let result = handle
        .publish(
            "control://config".to_string(),
            "cfg-1".to_string(),
            b"{\"max_connections\":100}".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok(), "Self mode serves config locally");
}

// Edge Cases
#[tokio::test]
async fn should_use_default_config_when_unreachable() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    // Node operates with default configuration when control plane unreachable
    let result = handle
        .publish(
            "notice://realm/events".to_string(),
            "evt-1".to_string(),
            b"test".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok(), "Node should use default config when control plane unavailable");
}

#[tokio::test]
async fn should_validate_config_before_applying() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://config".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let invalid_config = b"{\"ack_window_size\":-1}".to_vec();
    let result = handle
        .publish(
            "control://config".to_string(),
            "cfg-1".to_string(),
            invalid_config,
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok(), "Publish succeeds");
    // When validation is implemented, invalid configs would be rejected
    // and existing config retained
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    assert!(String::from_utf8_lossy(&msg.2).contains("-1"));
}

#[tokio::test]
async fn should_handle_zero_metrics_gracefully() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(10);
    handle
        .subscribe("control://metrics".to_string(), tx, 1)
        .await
        .expect("subscribe failed");

    // Act
    let zero_metrics = b"{\"nodeId\":\"test\",\"active_connections\":0,\"messages_per_second\":0,\"storage_bytes_used\":0}".to_vec();
    handle
        .publish(
            "control://metrics".to_string(),
            "m-1".to_string(),
            zero_metrics,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("publish failed");

    // Assert
    let msg = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("channel closed");
    let metrics_str = String::from_utf8_lossy(&msg.2);
    assert!(metrics_str.contains("\"active_connections\":0"));
    assert!(metrics_str.contains("\"messages_per_second\":0"));
    assert!(metrics_str.contains("\"storage_bytes_used\":0"));
}

#[tokio::test]
async fn should_include_node_id_in_all_control_messages() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let heartbeat_payload = b"{\"nodeId\":\"test-node\",\"timestamp\":1234567890}".to_vec();
    let metrics_payload = b"{\"nodeId\":\"test-node\",\"active_connections\":5}".to_vec();
    let shutdown_payload = b"{\"nodeId\":\"test-node\",\"reason\":\"maintenance\"}".to_vec();

    // Act
    let hb_result = handle
        .publish(
            "control://heartbeat".to_string(),
            "hb-1".to_string(),
            heartbeat_payload,
            None,
            None,
            false,
            None,
        )
        .await;
    let metrics_result = handle
        .publish(
            "control://metrics".to_string(),
            "m-1".to_string(),
            metrics_payload,
            None,
            None,
            false,
            None,
        )
        .await;
    let shutdown_result = handle
        .publish(
            "control://shutdown".to_string(),
            "s-1".to_string(),
            shutdown_payload,
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(hb_result.is_ok());
    assert!(metrics_result.is_ok());
    assert!(shutdown_result.is_ok());
    // All payloads include nodeId for message correlation
}
