# Queue Domain - Test Coverage

## Overview
Comprehensive test coverage for message queue operations extracted from `tests/queue.rs`.

## Test Inventory (47 tests)

### Basic Operations (7 tests)
- ✅ `should_enqueue_message_to_queue`
- ✅ `should_persist_enqueued_messages_durably`
- ✅ `should_receive_batch_from_queue`
- ✅ `should_return_lease_token_with_reserved_message`
- ✅ `should_make_reserved_message_invisible_to_other_consumers`
- ✅ `should_respect_visibility_timeout_on_lease`
- ✅ `should_support_batch_reserve`

### Message Lifecycle (5 tests)
- ✅ `should_complete_message_with_valid_token`
- ✅ `should_remove_completed_message_from_queue`
- ✅ `should_nack_message_and_return_to_queue`
- ✅ `should_not_increment_delivery_count_on_failed_consume`
- ✅ `should_make_nacked_message_available_immediately`

### Lease Management (5 tests)
- ✅ `should_extend_lease_with_valid_token`
- ✅ `should_prevent_message_return_when_lease_extended`
- ✅ `should_reject_extend_lease_with_invalid_token`
- ✅ `should_reject_extend_lease_after_expiration`
- ✅ `should_return_message_to_available_when_lease_expires`

### Peek Operations (3 tests)
- ✅ `should_peek_next_message_without_claiming`
- ✅ `should_allow_reserve_after_peek`
- ✅ `should_return_empty_when_peeking_empty_queue`

### Configuration (2 tests)
- ✅ `should_apply_queue_config_to_scope`
- ✅ `should_use_default_visibility_from_config`

### Deduplication (2 tests)
- ✅ `should_deduplicate_messages_with_same_dedupe_key`
- ✅ `should_allow_different_dedupe_keys`

### Error Handling (4 tests)
- ✅ `should_reject_complete_with_invalid_token`
- ✅ `should_reject_complete_for_nonexistent_message`
- ✅ `should_reject_double_complete`
- ✅ `should_return_empty_when_reserving_from_empty_queue`

### Dead Letter Queue (DLQ) (6 tests)
- ✅ `should_move_message_to_dlq_after_max_deliveries`
- ✅ `should_not_return_dlq_messages_in_normal_reserve`
- ✅ `should_allow_processing_dlq_messages_explicitly`
- ✅ `should_support_explicit_move_to_dlq`
- ✅ `should_preserve_message_ttl_when_moving_to_dlq_if_less_than_dlq_ttl`
- ✅ `should_apply_dlq_ttl_when_message_ttl_is_greater`
- ✅ `should_move_to_dlq_when_max_deliveries_exceeded`

### Metrics & Monitoring (3 tests)
- ✅ `should_track_in_flight_message_count`
- ✅ `should_decrease_in_flight_count_on_complete`
- ✅ (implicit in lease expiration test)

### FIFO Ordering (3 tests)
- ✅ `should_return_first_message_when_reserving_from_fifo_queue`
- ✅ `should_return_second_message_after_first_reserved`
- ✅ `should_return_third_message_after_first_two_reserved`

### Delivery Tracking (4 tests)
- ✅ `should_return_unique_token_on_each_delivery`
- ✅ `should_return_non_empty_token_on_first_delivery`
- ✅ `should_return_non_empty_token_on_redelivery`
- ✅ `should_track_delivery_count_on_redelivery`

### Concurrency (2 tests)
- ✅ `should_allow_concurrent_consumers_to_reserve_different_messages`
- ✅ `should_prevent_duplicate_delivery_to_concurrent_consumers`

## Implementation Status
- **Total Tests**: 47
- **Passing**: 0 (domain handler stubbed with panic!)
- **Blocked**: All tests blocked on domain implementation

## Next Steps
1. Implement QueueDomain::handle() to parse TLV and route to operations
2. Port logic from engine_old.rs or queue/service.rs
3. Update tests to work with new architecture
