# Lease Domain Specification

## Overview
Distributed lease management with expiration, extension, explicit release and optional queuing of requests.

## Route

lease://{realm}/{area}/{resource}


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

(Note: this domain follows the project's existing TLV helpers in `crate::protocol::frame` and `crate::protocol::tags`.)

### Operations
- **Acquire**: Obtain exclusive lease on resource
- **Extend**: Extend lease duration
- **Release**: Explicitly release lease
- Automatic expiration after duration

Operation selection: to remain compatible with existing engine helpers, operations are inferred from which TLV tags are present in the request payload (no separate `TAG_OP` is required):

- Acquire (a.k.a. reserve): request payload contains `TAG_LEASE` (u32 seconds). No `TAG_ID` or `TAG_DELIVERY_TOKEN` is required. If the resource is free the domain returns a response frame containing `TAG_ID`, `TAG_BODY` (optional payload), `TAG_DELIVERY_TOKEN` (string), and `TAG_LEASE` (remaining seconds).
- Extend: payload contains `TAG_ID` (string), `TAG_DELIVERY_TOKEN` (string), and `TAG_LEASE` (u32 seconds to add). Response: `TAG_LEASE` with remaining seconds.
- Release (consume): payload contains `TAG_ID` and `TAG_DELIVERY_TOKEN`. Response: empty frame or `TAG_OK` (optional). If queued requests exist, the domain will grant the next queued lease immediately.
- Peek: empty payload. Response: empty if no lease, otherwise `TAG_ID` and `TAG_BODY` for the currently-held lease (useful for diagnostics).

### Key Concepts
- **Lease Token**: HMAC-based token for authorization
- **Duration**: Time in seconds before automatic expiration
- **Mutual Exclusion**: Only one lease holder per resource


## Next Steps
1. Implement LeaseDomain::handle() to parse TLV and route to operations
2. Implement lease tracking data structure
3. Add background expiration task
4. Update tests to work with new architecture

-- Implementation checklist (details)

1) Data model

```
LeaseEntry {
	id: String,                 // lease id returned as TAG_ID
	token: String,              // base64 HMAC token
	expiry: Instant,            // absolute expiry time
	ttl_secs: u32,              // original TTL or last-set TTL
	body: Option<Vec<u8>>,      // optional payload returned to client in TAG_BODY
	waiters: VecDeque<Pending>, // queued acquire requests (FIFO)
}

Pending {
	requested_ttl: u32,
	responder: oneshot::Sender<Result<Vec<u8>, String>>, // engine compatible
}
```

2) Concurrency & storage

- Keep an in-memory `HashMap<String, LeaseEntry>` keyed by canonical resource route (realm/area/resource). Protect the top-level map with a `tokio::sync::Mutex` or use a `dashmap` for fine-grained concurrency. Each `LeaseEntry` may also hold its own `Mutex` if preferred.
- Use `tokio_util::time::DelayQueue` (or a binary heap) to efficiently track the next expiry and wake an expiration task to free leases.

3) Expiration task

- Spawn a background task at domain startup that awaits the next expiry from the DelayQueue. When a lease expires, remove it from the map and, if waiters exist, grant the next queued lease immediately.

4) Handler behavior (high level)

- Acquire (TAG_LEASE present):
	- If no active lease on the resource: create LeaseEntry, schedule expiry in DelayQueue, return TAG_ID, TAG_BODY, TAG_DELIVERY_TOKEN, TAG_LEASE (ttl)
	- If active lease exists: push requester to waiters queue and (depending on configured behavior) either return queued acknowledgement or block until lease is granted. For engine compatibility the `Engine::reserve` helper expects the handler to return a response payload directly — therefore the handler should only return to the caller when the lease is actually granted. (This simplifies the engine-facing API.)

- Extend: verify id+token, verify lease not expired, add TTL seconds to expiry (or set expiry = now + add_secs), update DelayQueue, return remaining seconds in TAG_LEASE.

- Release: verify id+token, delete the lease entry, if waiters exist then immediately grant the next waiter and return success.

5) Error handling & error codes

- Standard error strings (DomainResponse::Error carries a human string):
	- "invalid_token"
	- "lease_not_found"
	- "lease_expired"
	- "invalid_ttl"
	- "resource_unavailable" (used when not queueing)
	- "internal_error"

6) Tests mapping (implementation plan)

- Basic operations
	- should_acquire_lease_successfully — Acquire on free resource returns TAG_ID, TAG_DELIVERY_TOKEN and TTL > 0
	- should_return_lease_token_on_acquire — token present and well-formed (base64 length / can be parsed)
	- should_specify_lease_duration_on_acquire — TAG_LEASE in response matches requested TTL

- Lease extension
	- should_extend_active_lease — extend increases expiry; verify remaining seconds > previous remaining
	- should_return_new_expiry_time_on_extend — TAG_LEASE returned
	- should_allow_multiple_extensions — repeated extend calls accepted while active
	- should_prevent_expiration_when_extended_in_time — extend near expiry prevents expiration

- Lease release
	- should_release_lease_explicitly — release succeeds and frees resource
	- should_make_resource_available_after_release — a queued waiter is granted

- Expiration
	- should_expire_lease_after_duration — expiry frees resource; use accelerated timers in tests (tokio::time::pause/advance)
	- should_return_resource_to_pool_on_expiration — queued waiter receives lease on expiry

- Error handling
	- should_reject_extend_with_invalid_token
	- should_reject_release_with_invalid_token
	- should_reject_extend_on_expired_lease
	- should_reject_release_of_expired_lease
	- should_reject_lease_with_zero_duration
	- should_reject_extend_with_zero_duration

- Concurrency
	- should_prevent_concurrent_lease_acquisition — only one holder at a time
	- should_queue_lease_requests_when_resource_busy — FIFO waiter grant order

7) Implementation notes / pragmatic choices

- Start with in-memory only (no persistent store). If later required, store lease metadata in the KvStore so leases survive process restart (careful with expiry management across restarts).
- Use deterministic UUIDs for easier test assertions (allow injecting an id/token generator in tests).
- Use `tokio::time::Instant` and `DelayQueue` with `tokio_util` for expiry; in tests use `tokio::time::pause()`/`advance()` to simulate time.

8) Example TLV flows

- Acquire request (TTL=30s):

	- Request payload TLVs:
		- TAG_LEASE (u32) = 30

	- Successful response TLVs:
		- TAG_ID = "<lease-id>"
		- TAG_BODY = <optional blob>
		- TAG_DELIVERY_TOKEN = "<base64-hmac>"
		- TAG_LEASE = 30  (remaining seconds)

- Extend request (add 15s):

	- Request TLVs:
		- TAG_ID = "<lease-id>"
		- TAG_DELIVERY_TOKEN = "<base64-hmac>"
		- TAG_LEASE = 15

	- Success response TLVs:
		- TAG_LEASE = <remaining_seconds_after_extend>

- Release request:

	- Request TLVs:
		- TAG_ID = "<lease-id>"
		- TAG_DELIVERY_TOKEN = "<base64-hmac>"

	- Success: empty frame (or optional `TAG_OK`)

9) Observability & metrics

- Track counters: leases_acquired, leases_released, leases_expired, extend_calls, release_errors, invalid_token_errors, queued_requests.
- Export current size of lease map and wait queue lengths for monitoring.

10) Backwards compatibility & engine integration

- `Engine::reserve`, `extend_lease`, and `consume` helpers already build TLVs consistent with this design. Implement `LeaseDomain::handle()` to parse TLVs with `protocol::frame::find_tlv` and respect those helpers' expectations.

-- End of spec additions
