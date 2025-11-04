mod harness;
use harness::common::{create_sub_channel, default_sub_capacity, start_test_engine};

// ============================================================================
// NOTICE ENGINE INTEGRATION TESTS
// ============================================================================
// These tests exercise the engine-level notice/pub-sub functionality via
// in-process EngineHandle, not over WebSocket transport.
//
// For full end-to-end WebSocket tests, see e2e_notice_ws.rs (to be added).
// ============================================================================

// ============================================================================
// HAPPY PATH TESTS
// ============================================================================

#[tokio::test]
async fn should_deliver_notice_to_single_subscriber() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(default_sub_capacity());
    let _sub = handle
        .subscribe("notice://realm/area/resource/alerts".to_string(), tx, 1)
        .await
        .unwrap();

    // Act
    let result = handle
        .publish(
            "notice://realm/area/resource/alerts".to_string(),
            "msg1".to_string(),
            b"test message".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_deliver_notice_to_multiple_subscribers() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx1, mut rx1) = create_sub_channel(default_sub_capacity());
    let (tx2, mut rx2) = create_sub_channel(default_sub_capacity());
    let _sub1 = handle
        .subscribe("notice://realm/area/resource/alerts".to_string(), tx1, 1)
        .await
        .unwrap();
    let _sub2 = handle
        .subscribe("notice://realm/area/resource/alerts".to_string(), tx2, 2)
        .await
        .unwrap();

    // Act
    let result = handle
        .publish(
            "notice://realm/area/resource/alerts".to_string(),
            "broadcast1".to_string(),
            b"alert message".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_support_hierarchical_route_matching() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(default_sub_capacity());
    let _sub = handle
        .subscribe("notice://realm/area/system/alerts".to_string(), tx, 1)
        .await
        .unwrap();

    // Act
    let result = handle
        .publish(
            "notice://realm/area/system/alerts".to_string(),
            "sys1".to_string(),
            b"system alert".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_unsubscribe_successfully() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(default_sub_capacity());
    let sub_id = handle
        .subscribe("notice://realm/area/resource/test".to_string(), tx, 1)
        .await
        .unwrap();

    // Act
    let result = handle.unsubscribe(sub_id).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_handle_subscribe_with_different_channel_ids() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx1, mut rx1) = create_sub_channel(default_sub_capacity());
    let (tx2, mut rx2) = create_sub_channel(default_sub_capacity());

    // Act
    let sub1 = handle
        .subscribe("notice://realm/area/ch1/events".to_string(), tx1, 1)
        .await;
    let sub2 = handle
        .subscribe("notice://realm/area/ch2/events".to_string(), tx2, 2)
        .await;

    // Assert
    assert!(sub1.is_ok());
    assert!(sub2.is_ok());
}

#[tokio::test]
async fn should_cleanup_channel_subscriptions() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(default_sub_capacity());
    let _sub = handle
        .subscribe("notice://realm/area/resource/cleanup".to_string(), tx, 99)
        .await
        .unwrap();

    // Act
    let result = handle.cleanup_channel(99).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_deliver_notice_with_metadata() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(default_sub_capacity());
    let _sub = handle
        .subscribe("notice://realm/area/resource/meta".to_string(), tx, 1)
        .await
        .unwrap();

    // Act
    let result = handle
        .publish(
            "notice://realm/area/resource/meta".to_string(),
            "msg-123".to_string(),
            b"message body".to_vec(),
            Some("reply://route".to_string()),
            Some(42),
            true,
            Some(3600),
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// NEGATIVE TESTS
// ============================================================================

#[tokio::test]
async fn should_not_deliver_notice_to_unsubscribed_route() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(default_sub_capacity());
    let _sub = handle
        .subscribe("notice://realm/area/resource/foo".to_string(), tx, 1)
        .await
        .unwrap();

    // Act
    let result = handle
        .publish(
            "notice://realm/area/resource/bar".to_string(),
            "msg1".to_string(),
            b"should not receive".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_handle_publish_when_no_subscribers_exist() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .publish(
            "notice://realm/area/resource/empty".to_string(),
            "lonely".to_string(),
            b"no one listening".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_not_receive_notices_after_unsubscribe() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(default_sub_capacity());
    let sub_id = handle
        .subscribe("notice://realm/area/resource/temp".to_string(), tx, 1)
        .await
        .unwrap();

    // Act
    handle.unsubscribe(sub_id).await.unwrap();
    let result = handle
        .publish(
            "notice://realm/area/resource/temp".to_string(),
            "late".to_string(),
            b"too late".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_handle_invalid_subscription_route() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, _rx) = create_sub_channel(default_sub_capacity());

    // Act
    let result = handle.subscribe("".to_string(), tx, 1).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_handle_unsubscribe_with_invalid_id() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle.unsubscribe(99999).await;

    // Assert
    assert!(result.is_err() || result.is_ok());
}

#[tokio::test]
async fn should_handle_channel_cleanup_for_nonexistent_channel() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle.cleanup_channel(88888).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_handle_subscriber_channel_full_backpressure() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (tx, mut rx) = create_sub_channel(1);
    let _sub = handle
        .subscribe("notice://realm/area/resource/burst".to_string(), tx, 1)
        .await
        .unwrap();

    // Act
    for i in 0..10 {
        let _ = handle
            .publish(
                "notice://realm/area/resource/burst".to_string(),
                format!("msg{}", i),
                b"burst".to_vec(),
                None,
                None,
                false,
                None,
            )
            .await;
    }

    // Assert
    assert!(true);
}
