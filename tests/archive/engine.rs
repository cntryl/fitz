// // ENGINE CORE TESTS
// // These tests verify the engine's core responsibilities:
// //   1. Route parsing and extraction
// //   2. Route validation and authorization
// //   3. Dispatch to domain-specific handlers
// //
// // For domain-specific functionality tests, see:
// //   - control.rs   - Control plane operations
// //   - notice.rs    - Notice/PubSub
// //   - stream.rs    - Streams
// //   - rpc.rs       - RPC request/reply
// //   - queue.rs     - Queue operations
// //   - kv.rs        - Key-value store
// //   - lease.rs     - Lease coordination

// mod harness;
// use harness::common::start_test_engine;

// // ============================================================================
// // ROUTE PARSING & EXTRACTION
// // ============================================================================

// #[tokio::test]
// async fn should_extract_scheme_from_route() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "queue://realm/jobs".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Engine should parse "queue" scheme and dispatch to QueueDomain
//     // Currently panics because domain not implemented, but parsing should work
//     assert!(result.is_err()); // Expected until domain implemented
// }

// #[tokio::test]
// async fn should_extract_realm_from_route() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "kv://acme/config".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Engine should parse realm "acme" from route
//     assert!(result.is_err()); // Expected until domain implemented
// }

// #[tokio::test]
// async fn should_extract_area_from_route() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "stream://realm/orders".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Engine should parse area "orders" from route
//     assert!(result.is_err()); // Expected until domain implemented
// }

// #[tokio::test]
// async fn should_extract_resource_from_route() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "stream://realm/orders/order-123".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Engine should parse resource "order-123" from route
//     assert!(result.is_err()); // Expected until domain implemented
// }

// #[tokio::test]
// async fn should_handle_route_with_no_resource() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "kv://realm/config".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Engine should handle routes with just realm/area (no resource)
//     assert!(result.is_err()); // Expected until domain implemented
// }

// // ============================================================================
// // ROUTE VALIDATION
// // ============================================================================

// #[tokio::test]
// async fn should_reject_invalid_route_format() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "not-a-valid-route".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     assert!(result.is_err(), "Should reject malformed route");
// }

// #[tokio::test]
// async fn should_reject_route_with_unsupported_scheme() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "unsupported://realm/test".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     assert!(result.is_err(), "Should reject unknown scheme");
// }

// #[tokio::test]
// async fn should_reject_route_missing_realm() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "kv://".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     assert!(result.is_err(), "Should reject route without realm");
// }

// // ============================================================================
// // DOMAIN DISPATCH
// // ============================================================================

// #[tokio::test]
// async fn should_dispatch_queue_scheme_to_queue_domain() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "queue://realm/jobs".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Should reach QueueDomain handler (currently panics)
//     assert!(result.is_err());
//     // When implemented, verify error is from domain, not routing
// }

// #[tokio::test]
// async fn should_dispatch_kv_scheme_to_kv_domain() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "kv://realm/data".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Should reach KvDomain handler
//     assert!(result.is_err());
// }

// #[tokio::test]
// async fn should_dispatch_stream_scheme_to_stream_domain() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "stream://realm/events".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Should reach StreamDomain handler
//     assert!(result.is_err());
// }

// #[tokio::test]
// async fn should_dispatch_lease_scheme_to_lease_domain() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "lease://realm/resource-1".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Should reach LeaseDomain handler
//     assert!(result.is_err());
// }

// #[tokio::test]
// async fn should_dispatch_notice_scheme_to_notice_domain() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "notice://realm/events".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Should reach NoticeDomain handler
//     assert!(result.is_err());
// }

// #[tokio::test]
// async fn should_dispatch_control_scheme_to_control_domain() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "control://realm/config".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Should reach ControlDomain handler
//     assert!(result.is_err());
// }

// #[tokio::test]
// async fn should_dispatch_rpc_scheme_to_rpc_domain() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "rpc://realm/service".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Should reach RpcDomain handler
//     assert!(result.is_err());
// }

// #[tokio::test]
// async fn should_pass_payload_to_domain_handler() {
//     // Arrange
//     let (handle, _store) = start_test_engine();
//     use fitz::protocol::frame as fr;
    
//     let mut payload = Vec::new();
//     fr::build_tlv(fr::TAG_ID, b"test-id", &mut payload);
//     fr::build_tlv(fr::TAG_BODY, b"test-body", &mut payload);

//     // Act
//     let result = handle
//         .dispatch(
//             "kv://realm/data".to_string(),
//             payload,
//             0,
//         )
//         .await;

//     // Assert
//     // Payload should be passed to domain handler (currently panics)
//     assert!(result.is_err());
// }

// #[tokio::test]
// async fn should_pass_channel_id_to_domain_handler() {
//     // Arrange
//     let (handle, _store) = start_test_engine();
//     let channel_id = 42;

//     // Act
//     let result = handle
//         .dispatch(
//             "queue://realm/jobs".to_string(),
//             vec![],
//             channel_id,
//         )
//         .await;

//     // Assert
//     // Channel ID should be available to domain handler
//     assert!(result.is_err());
// }

// // ============================================================================
// // AUTHORIZATION
// // ============================================================================
// // Note: Authorization currently not enforced in test harness
// // These tests document expected behavior when authz is implemented

// #[tokio::test]
// async fn should_allow_dispatch_when_authz_not_enforced() {
//     // Arrange
//     let (handle, _store) = start_test_engine();

//     // Act
//     let result = handle
//         .dispatch(
//             "kv://realm/data".to_string(),
//             vec![],
//             0,
//         )
//         .await;

//     // Assert
//     // Currently allows all routes (authz not enforced in test)
//     assert!(result.is_err()); // Fails in domain, not authz
// }

// // ============================================================================
// // LIFECYCLE & OPERATIONAL
// // ============================================================================

// #[tokio::test]
// async fn should_handle_graceful_shutdown() {
//     // Arrange
//     let (handle, _store, jh) = harness::common::start_test_engine_with_join();

//     // Act
//     // Initiate shutdown by dropping handle
//     drop(handle);

//     // Assert
//     // Engine task completes cleanly
//     let result = tokio::time::timeout(std::time::Duration::from_secs(2), jh).await;
//     assert!(result.is_ok(), "Engine should shut down gracefully");
// }
