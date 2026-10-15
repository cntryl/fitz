use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn should_preserve_rpc_credit_across_cleanup_order_state_model(operations in prop::collection::vec(0_u8..5, 1..=48)) {
        // Arrange
        let h = Harness::new();
        let first = uuid::Uuid::new_v4();
        let queued = uuid::Uuid::new_v4();
        h.request(first);
        h.request(queued);
        let invocation = h.sink.worker_correlation_for_tests(h.family, first);
        h.cancel(first);
        let mut released = false;

        // Act
        for operation in operations {
            match operation {
                0..=2 => {
                    let family = if operation == 2 { RouteFamily::new(2) } else { h.family };
                    let worker = if operation == 1 { 99 } else { 42 };
                    let frame = encode_cancel_ack_tlv_frame(&invocation);
                    let (kind, payload) = crate::benchkit::extract_single_tlv_field(&frame);
                    h.sink.deliver(Envelope::from_route(
                        session_inbox_address(family, worker), RouteAddress::new(family, h.route.clone()),
                        rpc_frame(worker, family, kind, payload),
                    )).expect("cleanup ack");
                    if operation == 0 { released = true; }
                }
                3 => {
                    let response = crate::domains::rpc::protocol::RpcResponse::single(invocation, Bytes::from_static(b"late completion"));
                    let payload = crate::dispatch::protocol::rpc_codec::encode_response_message(&response);
                    h.sink.deliver(Envelope::from_route(
                        session_inbox_address(h.family, 42), RouteAddress::new(h.family, h.route.clone()),
                        rpc_frame(42, h.family, 303, Bytes::from(payload)),
                    )).expect("late completion");
                }
                _ => h.cancel(first),
            }

            // Assert
            prop_assert_eq!(h.capacity(), if released { 1 } else { 2 }, "after operation {}", operation);
            prop_assert_eq!(h.worker_request_count(), if released { 2 } else { 1 });
        }
        h.ack(first);
        prop_assert_eq!(h.capacity(), 1);
        prop_assert_eq!(h.worker_request_count(), 2);
        h.cancel(queued);
        h.ack(queued);
        prop_assert_eq!(h.capacity(), 0);
    }
}
