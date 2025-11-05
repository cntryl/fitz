# Lease Domain Specification

## Overview
Distributed lease management with expiration, extension, and explicit release.

## Test Coverage

### Basic Operations
- `should_acquire_lease_successfully`
- `should_return_lease_token_on_acquire`
- `should_specify_lease_duration_on_acquire`

### Lease Extension
- `should_extend_active_lease`
- `should_return_new_expiry_time_on_extend`
- `should_allow_multiple_extensions`
- `should_prevent_expiration_when_extended_in_time`

### Lease Release
- `should_release_lease_explicitly`
- `should_make_resource_available_after_release`

### Expiration
- `should_expire_lease_after_duration`
- `should_return_resource_to_pool_on_expiration`

### Error Handling
- `should_reject_extend_with_invalid_token`
- `should_reject_release_with_invalid_token`
- `should_reject_extend_on_expired_lease`
- `should_reject_release_of_expired_lease`
- `should_reject_lease_with_zero_duration`
- `should_reject_extend_with_zero_duration`

### Concurrency
- `should_prevent_concurrent_lease_acquisition`
- `should_queue_lease_requests_when_resource_busy`

## Protocol Details

### TLV Tags
- `TAG_ID` - Resource ID
- `TAG_DELIVERY_TOKEN` - Lease token
- `TAG_LEASE` - Lease duration in seconds

### Operations
- **Acquire**: Obtain exclusive lease on resource
- **Extend**: Extend lease duration
- **Release**: Explicitly release lease
- Automatic expiration after duration

### Key Concepts
- **Lease Token**: HMAC-based token for authorization
- **Duration**: Time in seconds before automatic expiration
- **Mutual Exclusion**: Only one lease holder per resource
