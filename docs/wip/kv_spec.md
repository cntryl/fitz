# KV Domain Specification

## Overview
Key-value store with scan, batch operations, and range deletion.

## Test Coverage

### Basic Operations
- `should_put_key_value_pair`
- `should_overwrite_existing_key`
- `should_retrieve_new_value_after_overwrite`
- `should_get_value_for_existing_key`
- `should_return_none_for_nonexistent_key`
- `should_delete_existing_key`
- `should_return_none_after_key_deleted`
- `should_handle_delete_of_nonexistent_key`

### Scan Operations
- `should_scan_keys_from_start_key`
- `should_respect_scan_limit`
- `should_scan_in_lexicographic_order`
- `should_return_empty_when_start_key_beyond_all_keys`

### Batch Operations
- `should_put_multiple_keys_in_batch`
- `should_retrieve_all_keys_after_batch_put`
- `should_commit_batch_put_atomically`
- `should_make_all_keys_available_after_batch_put`
- `should_get_multiple_keys_in_batch`
- `should_return_none_for_missing_keys_in_batch`

### Range Operations
- `should_delete_range_of_keys`
- `should_preserve_keys_before_delete_range_start`
- `should_delete_keys_within_range_inclusive_start`
- `should_delete_keys_within_range_middle`
- `should_preserve_keys_at_delete_range_end`
- `should_handle_delete_range_with_no_matching_keys`

### Route Isolation
- `should_isolate_keys_by_route_config`
- `should_isolate_keys_by_route_resource`
- `should_not_find_key_in_different_route`

### Edge Cases
- `should_store_empty_value`
- `should_retrieve_empty_value_for_existing_key`
- `should_return_none_for_missing_key_when_empty_value_exists`
- `should_store_large_value`
- `should_retrieve_large_value`
- `should_reject_value_exceeding_max_size`

### Special Characters
- `should_handle_keys_with_slashes`
- `should_handle_keys_with_colons`
- `should_handle_keys_with_at_symbols`
- `should_handle_unicode_keys`

### Concurrency
- `should_handle_concurrent_puts_to_same_key`
- `should_store_value_after_concurrent_puts`
- `should_complete_concurrent_get_without_error`
- `should_complete_concurrent_delete_without_error`

## Protocol Details

### TLV Tags
- `TAG_ID` - Key
- `TAG_BODY` - Value
- Operation indicated by route/method

### Operations
- **Put**: Store key-value pair
- **Get**: Retrieve value by key
- **Delete**: Remove key
- **Scan**: List keys from start position
- **Batch Put**: Atomic multi-key insert
- **Batch Get**: Retrieve multiple keys
- **Delete Range**: Remove keys in range [start, end)
