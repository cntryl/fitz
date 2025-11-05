# Stream Domain - Test Coverage

## Overview
Comprehensive test coverage for event stream operations extracted from `tests/stream.rs`.

## Test Inventory (106 tests)

### Basic Append (3 tests)
- ✅ `should_append_event_with_resource_sequence_zero`
- ✅ `should_append_resource_sequence_one_after_zero`
- ✅ `should_append_resource_sequence_two_after_one`

### Finalization & Area Sequences (8 tests)
- ✅ `should_assign_area_sequences_on_finalization`
- ✅ `should_assign_correct_area_sequence_count_on_finalization`
- ✅ `should_not_appear_in_area_until_finalized`
- ✅ `should_remain_visible_in_resource_before_finalization`
- ✅ `should_appear_in_area_atomically_on_finalization`
- ✅ `should_not_appear_in_area_before_finalization`
- ✅ `should_not_assign_area_seq_before_finalization`
- ✅ `should_not_assign_area_seq_before_finalization` (duplicate)

### Read Operations (17 tests)
- ✅ `should_read_all_events_from_stream`
- ✅ `should_return_last_event_with_correct_sequence`
- ✅ `should_return_last_event_with_correct_body`
- ✅ `should_peek_without_advancing_offset`
- ✅ `should_read_from_fully_qualified_route`
- ✅ `should_read_correct_body_from_fully_qualified_route`
- ✅ `should_read_events_starting_from_sequence_two`
- ✅ `should_read_correct_sequence_numbers_from_offset`
- ✅ `should_respect_read_limit`
- ✅ `should_read_events_in_append_order`
- ✅ `should_read_first_event_body_correctly`
- ✅ `should_read_second_event_body_correctly`
- ✅ `should_read_third_event_body_correctly`
- ✅ `should_read_from_beginning_when_fromseq_zero`
- ✅ `should_read_resource_stream_independent_of_watermark`
- ✅ `should_read_area_stream_respecting_watermark`
- ✅ `should_read_resource_stream_independent_of_finalization`

### Prefix/Area Reads (6 tests)
- ✅ `should_consume_from_prefix_route`
- ✅ `should_interleave_events_from_multiple_streams`
- ✅ `should_merge_descendants_in_deterministic_order`
- ✅ `should_consume_with_fromseq_and_limit`
- ✅ `should_consume_returns_events`
- ✅ `should_consume_returns_area_seq`
- ✅ `should_consume_returns_event_body`

### Expected Revision (6 tests)
- ✅ `should_append_when_expected_revision_matches`
- ✅ `should_append_with_any_revision`
- ✅ `should_append_when_stream_empty_with_no_stream_expected`
- ✅ `should_reject_append_when_expected_revision_mismatch`
- ✅ `should_reject_append_to_existing_stream_with_no_stream_expected`
- ✅ `should_reject_append_when_stream_exists_but_expecting_empty`

### Error Handling (7 tests)
- ✅ `should_return_empty_when_peeking_nonexistent_stream`
- ✅ `should_reject_peek_with_prefix_route`
- ✅ `should_return_empty_when_reading_nonexistent_stream`
- ✅ `should_return_empty_when_fromseq_beyond_end`
- ✅ `should_handle_zero_limit_in_read`
- ✅ `should_return_empty_when_consuming_nonexistent_prefix`
- ✅ `should_handle_consume_with_no_descendants`

### Ordering Guarantees (4 tests)
- ✅ `should_maintain_order_under_sequential_appends`
- ✅ `should_preserve_sequence_numbers_under_sequential_appends`
- ✅ `should_preserve_append_order_in_read`
- ✅ `should_enforce_monotonic_resource_sequences`

### Large Payloads (4 tests)
- ✅ `should_accept_large_payload_append`
- ✅ `should_preserve_large_payload_size_on_read`
- ✅ `should_reject_payload_exceeding_max_size`
- ✅ `should_handle_read_with_large_limit`

### Batch/Gap Detection (13 tests)
- ✅ `should_reserve_sequential_area_sequences_for_batch`
- ✅ `should_block_visibility_until_batch_commits`
- ✅ `should_return_events_when_batches_commit`
- ✅ `should_advance_watermark_when_batches_commit`
- ✅ `should_not_advance_watermark_past_uncommitted_gap`
- ✅ `should_handle_interleaved_commit_order`
- ✅ `should_maintain_watermark_for_orders_area`
- ✅ `should_maintain_watermark_for_payments_area`
- ✅ `should_handle_concurrent_small_appends_without_gaps`
- ✅ `should_handle_out_of_order_commits_correctly`
- ✅ `should_handle_large_batch_blocking_many_small_batches`
- ✅ `should_report_watermark_in_read_response`
- ✅ (various watermark tests)

### Duplicate/Gap Detection (10 tests)
- ✅ `should_reject_duplicate_resource_seq_in_batch`
- ✅ `should_reject_batch_with_sequence_gap`
- ✅ `should_allow_batch_retry_with_same_sequences`
- ✅ `should_reject_batch_retry_with_different_bodies`
- ✅ `should_reject_gap_in_resource_sequence`
- ✅ `should_allow_idempotent_retry_on_same_resource_seq`
- ✅ `should_reject_resource_seq_with_different_body`
- ✅ `should_skip_area_sequence_gaps_in_watermark`
- ✅ `should_track_rolled_back_area_sequences`
- ✅ `should_maintain_ordering_across_area_sequence_gaps`

### Stream Closure (2 tests)
- ✅ `should_handle_batch_with_end_marker`
- ✅ `should_reject_append_after_stream_closed`

## Implementation Status
- **Total Tests**: 106
- **Passing**: 0 (domain handler stubbed with panic!)
- **Blocked**: All tests blocked on domain implementation

## Complexity Notes
Stream is the most complex domain with:
- Dual sequence tracking (resource + area)
- Gap detection and watermark management
- Finalization atomicity
- Batch operations with rollback
- Idempotency and deduplication

## Next Steps
1. Implement StreamDomain::handle() to parse TLV and route to operations
2. Port complex gap detection logic from engine_old.rs
3. Update tests to work with new architecture
