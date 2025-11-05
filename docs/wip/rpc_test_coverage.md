# RPC Domain - Test Coverage

## Overview
Comprehensive test coverage for RPC operations extracted from `tests/rpc.rs`.

## Test Inventory (48 tests)

### Basic RPC (3 tests)
- ✅ `should_deliver_rpc_request_to_handler`
- ✅ `should_deliver_reply_to_specified_reply_route`
- ✅ `should_correlate_reply_with_request_id`

### Inbox Management (12 tests)
- ✅ `should_allocate_inbox_when_reply_route_omitted`
- ✅ `should_generate_cryptographically_secure_inbox_routes`
- ✅ `should_prevent_inbox_route_collision`
- ✅ `should_prevent_unauthorized_inbox_subscription`
- ✅ `should_allow_owner_to_receive_on_inbox`
- ✅ `should_isolate_inbox_from_other_sessions`
- ✅ `should_reject_unauthorized_inbox_publish`
- ✅ `should_prevent_delivery_from_unauthorized_sender`
- ✅ `should_allow_handler_to_publish_to_reply_inbox`
- ✅ `should_deliver_handler_reply_to_client`
- ✅ `should_prevent_inbox_access_after_session_ends`
- ✅ `should_cleanup_allocated_inboxes_after_session_close`

### Streaming Responses (4 tests)
- ✅ `should_deliver_streaming_rpc_responses_in_order`
- ✅ `should_mark_end_of_stream_with_stream_end_tag`
- ✅ `should_handle_multiple_chunks_in_streaming_response`
- ✅ `should_stream_large_response_in_chunks`

### Concurrency (2 tests)
- ✅ `should_handle_concurrent_rpc_calls`
- ✅ `should_isolate_replies_by_correlation_id`

### RPC Client (3 tests)
- ✅ `should_use_rpc_client_for_call_stream`
- ✅ `should_manage_reply_route_subscription_automatically`
- ✅ (client wrapper tests)

### Error Handling (9 tests)
- ✅ `should_handle_rpc_request_when_no_handler_subscribed`
- ✅ `should_timeout_when_no_reply_received`
- ✅ `should_reject_rpc_to_invalid_route`
- ✅ `should_reject_reply_without_correlation_id`
- ✅ `should_handle_out_of_order_sequence_numbers`
- ✅ `should_handle_missing_sequence_number`
- ✅ `should_propagate_application_errors_in_reply`
- ✅ `should_handle_handler_crash_during_request_processing`
- ✅ (various error modes)

### Custom Configuration (3 tests)
- ✅ `should_support_custom_inbox_reply_routes`
- ✅ `should_respect_client_specified_timeout`
- ✅ `should_use_default_timeout_when_not_specified`

### Large Payloads (2 tests)
- ✅ `should_handle_large_rpc_request_payload`
- ✅ `should_handle_large_rpc_reply_payload`

### Load Balancing (2 tests)
- ✅ `should_distribute_requests_across_multiple_handlers`
- ✅ `should_ensure_single_handler_receives_each_request`

### Cancellation & Idempotency (4 tests)
- ✅ `should_support_request_cancellation`
- ✅ `should_not_deliver_reply_after_cancellation`
- ✅ `should_support_idempotent_request_ids`
- ✅ `should_deduplicate_requests_by_id`

## Implementation Status
- **Total Tests**: 48
- **Passing**: 0 (domain handler stubbed with panic!)
- **Blocked**: All tests blocked on domain implementation

## Special Considerations
- RPC requires coordination with notice domain for subscriptions
- Inbox lifecycle tied to session/channel cleanup
- Security critical: inbox authorization must be enforced

## Next Steps
1. Implement RpcDomain::handle() to parse TLV and route to operations
2. Integrate with Router for pub/sub mechanics
3. Implement inbox security model
4. Update tests to work with new architecture
