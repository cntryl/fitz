
### AC-SCHEDULE-001: Create Scheduled Job

**MUST** create job with cron expression
**Given:** Session with `schedule://realm/area/**#write` permission  
**When:** Client sends `Create(route="schedule://prod/jobs/backup", cron="0 2 * * *", delivery_mode="broadcast", payload="backup-db")`
**Then:**

- Server returns `CreateOk(job_id)`
- Job is persisted
- Job will trigger at 2:00 AM daily

### AC-SCHEDULE-002: Cron Expression Validation

**MUST** reject invalid cron expressions
**Given:** Client attempts to create job  
**When:** Client sends `Create(cron="invalid syntax")`  
**Then:**

- Server returns a status=1 error with code `7051` (ERR_INVALID_CRON)
- Job is NOT created

### AC-SCHEDULE-003: Job Execution Notification via SCHEDULE_SUBSCRIBE / SCHEDULE_NOTIFY

**MUST** receive notification when schedule fires
**Given:**

- Job created with cron `"*/1 * * * *"` (every minute) on route `schedule://prod/app/reminders/send`
- Client sends `SCHEDULE_SUBSCRIBE` (703) to `schedule://prod/app/reminders/*`
  **When:** Time advances to next minute boundary  
  **Then:**
- Client receives `SCHEDULE_NOTIFY` (705) with the exact fired route and the
  job's configured payload
- Payload matches job's configured payload
- Notification arrives within 1 second of scheduled time

### AC-SCHEDULE-004: Cancel Job (renumbered)

**MUST** cancel scheduled job
**Given:** Job exists with `job_id=J1`  
**When:** Client sends `Cancel(job_id=J1)`  
**Then:**

- Server returns `CancelOk`
- Job no longer fires
- Future scheduled times do not trigger notifications

### AC-SCHEDULE-005: List Jobs (renumbered)

**MUST** retrieve all jobs for realm/area
**Given:** Jobs exist for `schedule://prod/jobs/*`  
**When:** Client sends `List(realm="prod", area="jobs")`  
**Then:**

- Server returns list of jobs with:
  - Job ID
  - Cron expression
  - Next scheduled time
- Payload
  - Delivery mode

### AC-SCHEDULE-006: Cron Wildcards (renumbered)

**MUST** support wildcard expressions
**Given:** Job with cron `"* * * * *"` (every minute)  
**When:** Time advances through multiple minutes  
**Then:**

- Job fires every minute
- No missed executions (within 1s tolerance)

### AC-SCHEDULE-007: Cron Ranges and Lists (renumbered)

**MUST** support range and list syntax
**Given:** Job with cron `"0 9-17 * * 1-5"` (9 AM to 5 PM, Mon-Fri)  
**When:** Time is Monday 10:00 AM  
**Then:** Job fires
**When:** Time is Saturday 10:00 AM  
**Then:** Job does NOT fire
**When:** Time is Monday 8:00 AM  
**Then:** Job does NOT fire

### AC-SCHEDULE-008: Unauthorized Create (renumbered)

**MUST** reject job creation without write permission
**Given:** Session JWT has no `schedule:write` scope  
**When:** Client sends `Create(route, cron, payload)`  
**Then:**

- Server returns error code `7001` (ERR_UNAUTHORIZED)
- Job NOT created

## Error Handling

### AC-ERROR-001: Protocol Parse Errors

**MUST** handle malformed frames gracefully
**Given:** Client sends a frame whose header `length` disagrees with the bytes present, or sets a reserved flag bit  
**When:** Server receives malformed frame  
**Then:**

- Server closes connection with a protocol error
- Client logs error and does NOT retry same malformed data
- **One message per frame.** A frame carries exactly one 15-byte header and its value; the client **MUST NOT** pack a second message after the first.

### AC-ERROR-002: Domain Error Codes

**MUST** correctly parse domain-specific error codes
**Given:** Client sends unauthorized operation  
**When:** Server returns error with domain-specific code (e.g., `4001` for Queue)  
**Then:**

- Client recognizes error code format: `XXYY` where `XX` = domain, `YY` = error
- Client maps to appropriate error type (Unauthorized)
- Client does NOT misinterpret as different error

### Error Code Ranges (Normative)

| Domain   | Code range |
| -------- | ---------- |
| KV       | 1000-1999  |
| Stream   | 2000-2999  |
| Notice   | 3000-3999  |
| Queue    | 4000-4999  |
| Lease    | 5000-5999  |
| RPC      | 6000-6999  |
| Schedule | 7000-7999  |

Clients **MUST** interpret error codes using this mapping.

### AC-ERROR-003: Retryable vs Fatal Errors

**MUST** distinguish retryable from fatal errors
**Given:** Client encounters error  
**When:** Error code is:

- `1050` (Transaction Not Found) → Fatal, do NOT retry
- `6007` (ERR_TIMEOUT; accepted but not finished before the deadline; effect INDETERMINATE) → Retryable with backoff only when the operation is idempotent
- `6051` (ERR_ROUTE_NOT_REGISTERED; no workers registered for route) → Retryable with backoff
- `7005` (Schedule ERR_BUSY; declined before the domain saw it, nothing applied) → Retryable with backoff
- `1001` (KV Unauthorized) → Fatal, do NOT retry
- `2001` (Stream Unauthorized) → Fatal, do NOT retry
- `4001` (Queue Unauthorized) → Fatal, do NOT retry
- `5001` (Lease Unauthorized) → Fatal, do NOT retry
- `6001` (RPC Unauthorized) → Fatal, do NOT retry
- `7001` (Schedule Unauthorized) → Fatal, do NOT retry
  **Then:**
- Client retries only retryable errors
- Client uses exponential backoff for retries
- Client fails fast on fatal errors

### AC-ERROR-004: Connection Loss Recovery

**MUST** recover from connection loss
**Given:** Active connection with in-flight operations  
**When:** Network connection drops  
**Then:**

- Client detects disconnection within 5 seconds
- Client attempts reconnection with exponential backoff
- Client re-authenticates with CONNECT frame
- Client re-establishes reconnect-safe subscriptions and RPC worker registrations
- Client invalidates session-bound handles and pending operations according to AC-CONN-006

## Performance

### AC-PERF-001: Frame Size Limits

**MUST** respect `max_message_bytes` from `HELLO` (1 MiB by default)
**Given:** Client attempts to send large payload  
**When:** Payload exceeds `max_message_bytes` (1 MiB by default)
**Then:**

- Client either:
  - Rejects operation before sending, OR
  - Server closes the connection
- Client splits large data across multiple operations
  **Chunking notes:**
- **RPC** supports explicit chunked responses (see AC-RPC-005).
- **Stream READ** returns one bounded response page; clients request the next
  page with the returned cursor and MUST NOT rely on implicit frame reassembly.
- Other domains (e.g., KV, Queue) should use multiple logical operations or application-level chunking; clients MUST NOT rely on implicit chunk reassembly in those domains.
  **Configuration:**
- Server default: 1 MB (configurable via `BootConfig::max_frame_size`)
- Client SDK limit must be configurable to match the server limit
- Test environments: May use larger limits (e.g., 16 MB) for convenience

### AC-PERF-002: Connection Pooling

**SHOULD** reuse connections efficiently
**Given:** Client makes multiple operations  
**When:** Operations occur within short time window  
**Then:**

- Client reuses same WebSocket connection
- Client does NOT create new connection per operation
- Client maintains connection pool (if multi-threaded)

### AC-PERF-003: Backpressure Handling

**MUST** handle backpressure signals
**Given:** Server experiencing high load  
**When:** Server responds with rate-limit or backpressure error codes (or an explicit backpressure frame)  
**Then:**

- Client pauses sending
- Client applies exponential backoff
- Client does NOT flood server with retries

### AC-PERF-004: Subscription Throughput

**SHOULD** handle high-volume subscriptions
**Given:** Client subscribed to high-traffic route (1000+ msg/sec)  
**When:** Messages arrive rapidly  
**Then:**

- Client processes messages without blocking
- Client does NOT accumulate unbounded backlog
- Client drops messages if processing can't keep up (with logging)

### AC-PERF-005: Latency Measurement

**SHOULD** track operation latency
**Given:** Client performs operations  
**When:** Client tracks time from send to response  
**Then:**

- Client exposes latency metrics (p50, p95, p99)
- Client logs slow operations (> 1s)
- Client can identify performance regressions

### AC-PERF-006: Bounded Concurrency Under Burst Load

**MUST** cap in-flight work under burst load
**Given:** Client configured with a finite max-in-flight limit and a burst of concurrent requests above that limit
**When:** Client issues the burst across one or more domains
**Then:**

- Active in-flight requests or handler tasks never exceed the configured limit
- Excess work is queued, blocked, or backpressured; client does NOT spawn unbounded goroutines, tasks, or promises
- Backpressure or retryable overload errors are surfaced, or the client waits until capacity frees before retrying
- Closing the client during load releases background work and returns the client to baseline

## Appendix: Error Code Reference

This appendix provides a complete reference of all Fitz error codes by domain, as required by AC-ERROR-002.

### Error Code Format

Error codes follow the format `XXYY` where:
- `XX` = Domain identifier (10-79)
- `YY` = Domain-specific error number (01-99)

### KV Domain (1000-1999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 1050 | ERR_TRANSACTION_NOT_FOUND | Transaction ID does not exist or expired | No |
| 1051 | ERR_INVALID_MODE | Invalid transaction mode specified | No |
| 1052 | ERR_KEY_NOT_FOUND | Key does not exist in transaction view | No |
| 1053 | ERR_ISOLATION_CONFLICT | Read-write set conflict detected | Yes (with backoff) |
| 1054 | ERR_WRITE_IN_READONLY | Write attempted in read-only transaction | No |
| 1055 | ERR_KEY_EXISTS | Key already exists (Insert failed) | No |
| 1002 | ERR_INVALID_ROUTE | Route format invalid or malformed | No |
| 1056 | ERR_REALM_MISMATCH | Operation crosses realm boundaries | No |
| 1006 | ERR_BACKEND_ERROR | Storage backend error | Yes (with backoff) |
| 1057 | ERR_TRANSACTION_ABORTED | Transaction aborted by system | No |
| 1001 | ERR_UNAUTHORIZED | Permission denied for KV operation | No |
| 1003 | ERR_INVALID_SUBSCRIPTION_PATTERN | KV subscription pattern syntax or depth invalid | No |
| 1004 | ERR_SUBSCRIPTION_LIMIT | Session exceeded 1,024 total or 128 wildcard KV registrations | No |

### Stream Domain (2000-2999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 2050 | ERR_CONCURRENCY_CONFLICT | Expected offset does not match (AC-STREAM-013) | No |
| 2051 | ERR_SESSION_ALREADY_ACTIVE | Another append session is already active for that resource | No |
| 2052 | ERR_SESSION_NOT_FOUND | Session ID is missing, stale, or already cleaned up | No |
| 2053 | ERR_INVALID_READ_BOUND | Read range bounds invalid | No |
| 2054 | ERR_RESOURCE_NOT_FOUND | Stream resource does not exist | No |
| 2055 | ERR_STREAM_FILTER_UNSUPPORTED_VERSION | Filter marker/version is not supported by this broker | No |
| 2056 | ERR_STREAM_FILTER_INVALID_PAYLOAD | Filter payload malformed or undecodable | No |
| 2001 | ERR_UNAUTHORIZED | Permission denied for stream operation | No |
| 2003 | ERR_INVALID_SUBSCRIPTION_PATTERN | Subscription pattern syntax invalid | No |
| 2004 | ERR_SUBSCRIPTION_LIMIT | Session exceeded 1,024 total or 128 wildcard Stream registrations | No |
| 2006 | ERR_BACKEND_ERROR | Stream storage backend error | Yes (with backoff) |
| 2057 | ERR_READ_RESPONSE_TOO_LARGE | A single record's wire-encoded size alone exceeds the maximum response frame size and can never be returned by any READ | No |

### Notice Domain (3000-3999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 3002 | ERR_INVALID_ROUTE | Notice route format invalid | No |
| 3003 | ERR_INVALID_PATTERN | Subscription pattern syntax invalid | No |
| 3004 | ERR_SUBSCRIPTION_LIMIT | Session exceeded 1,024 total or 128 wildcard Notice registrations | No |
| 3050 | ERR_TRANSPORT_CLOSED | Transport connection closed | No |
| 3006 | ERR_BACKEND_ERROR | Notice broker-side processing error | Yes (with backoff) |
| 3001 | ERR_UNAUTHORIZED | Permission denied for notice operation | No |

### Queue Domain (4000-4999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 4050 | ERR_INVALID_TOKEN | Queue inflight token invalid or wrong (AC-QUEUE-006) | No |
| 4051 | ERR_INFLIGHT_EXPIRED | Message inflight reservation expired | No |
| 4052 | ERR_MESSAGE_NOT_FOUND | Message ID not found in queue | No |
| 4053 | ERR_QUEUE_NOT_FOUND | Queue resource does not exist | No |
| 4005 | ERR_BUSY | Broker declined the request; nothing applied | Yes (with backoff) |
| 4008 | ERR_BAD_REQUEST | Malformed Queue operation request | No |
| 4006 | ERR_BACKEND_ERROR | Queue storage backend error | Yes (with backoff) |
| 4001 | ERR_UNAUTHORIZED | Permission denied for queue operation | No |
| 4003 | ERR_INVALID_SUBSCRIPTION_PATTERN | Queue subscription pattern syntax or depth invalid | No |
| 4004 | ERR_SUBSCRIPTION_LIMIT | Session exceeded 1,024 total or 128 wildcard Queue registrations | No |

### Lease Domain (5000-5999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 5050 | ERR_LEASE_HELD | Lease already held by another client | Yes (with backoff) |
| 5051 | ERR_INVALID_FENCE | Fencing token invalid or out of order | No |
| 5052 | ERR_LEASE_EXPIRED | Lease TTL expired | No |
| 5053 | ERR_LEASE_NOT_FOUND | Lease resource does not exist | No |
| 5054 | ERR_INVALID_TOKEN | Lease token invalid or wrong (AC-LEASE-009) | No |
| 5055 | ERR_ACQUIRE_WAIT_EXPIRED | The caller's `wait_seconds` elapsed without a grant | Yes, if still needed |
| 5056 | ERR_WAITERS_LIMIT | Too many pending waiters on the key | Yes (with backoff) |
| 5008 | ERR_BAD_REQUEST | Malformed Lease operation request | No |
| 5001 | ERR_UNAUTHORIZED | Permission denied for lease operation | No |
| 5003 | ERR_INVALID_PATTERN | Lease SUBSCRIBE, UNSUBSCRIBE, or LIST selector fails the shared exact-or-wildcard grammar | No |
| 5057 | ERR_INVALID_LIST_CURSOR | LIST cursor does not match the selector, `RouteFamily`, or broker lifetime it was issued from | No |

### RPC Domain (6000-6999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 6007 | ERR_TIMEOUT | Accepted but not finished before the deadline; effect INDETERMINATE | Yes, if safe (with backoff) |
| 6050 | ERR_WORKER_NOT_FOUND | Assigned worker disconnected or unregistered before completion | Yes, if safe (with backoff) |
| 6005 | ERR_BUSY | Broker declined the request; nothing applied | Yes (with backoff) |
| 6051 | ERR_ROUTE_NOT_REGISTERED | No workers registered for route (AC-RPC-003) | Yes (with backoff) |
| 6052 | ERR_CORRELATION_NOT_FOUND | Response, ACK, or cancellation referenced an unknown or finished correlation | No |
| 6053 | ERR_RPC_INVALID_SEQUENCE | Response chunks did not start at sequence 0 or advance contiguously | No |
| 6054 | ERR_RPC_DUPLICATE_CORRELATION | Caller reused a correlation ID that is still live | Only with a new correlation ID if safe |
| 6055 | ERR_RPC_WRONG_WORKER | Response or ACK came from a worker that does not own the request | No |
| 6001 | ERR_UNAUTHORIZED | Permission denied for RPC operation | No |
| 6006 | ERR_BACKEND_ERROR | Broker-side parse or backend failure while handling the RPC | Only when the error text indicates a transient backend or infrastructure failure |
| 6002 | ERR_INVALID_ROUTE | RPC call route is malformed or contains a wildcard | No |
| 6003 | ERR_INVALID_SUBSCRIPTION_PATTERN | RPC worker registration pattern syntax invalid | No |
| 6004 | ERR_SUBSCRIPTION_LIMIT | Session exceeded 1,024 total or 128 wildcard RPC registrations | No |

### Schedule Domain (7000-7999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 7050 | ERR_SCHEDULE_NOT_FOUND | Schedule job does not exist | No |
| 7051 | ERR_INVALID_CRON | Cron expression syntax invalid (AC-SCHEDULE-002) | No |
| 7052 | ERR_DEFINITION_LIMIT | Maximum schedules reached | No |
| 7008 | ERR_BAD_REQUEST | Schedule payload violates the verb contract | No |
| 7053 | ERR_INVALID_TARGET | Target route invalid or unsupported | No |
| 7003 | ERR_INVALID_SUBSCRIPTION_PATTERN | Subscription pattern syntax invalid | No |
| 7004 | ERR_SUBSCRIPTION_LIMIT | Session exceeded 1,024 total or 128 wildcard Schedule registrations | No |
| 7054 | ERR_INVALID_DELIVERY_MODE | Delivery mode is not broadcast (0) or single (1) | No |
| 7001 | ERR_UNAUTHORIZED | Permission denied for schedule operation | No |
| 7005 | ERR_BUSY | Broker declined the request before the domain saw it; nothing applied | Yes (with backoff) |

Every Schedule verb answers errors in the common envelope; see
`docs/clients/spec/schedule.md`. 7001, 7005, and 7007 are raised by the broker
before dispatch. Retryability on every table above is derived from the
envelope's `effect`; the column is a summary, not the contract.

### Error Handling Guidelines

**Retryable Errors:**
- Implement exponential backoff (e.g., 100ms, 200ms, 400ms, 800ms, max 5s)
- Limit retry attempts (e.g., max 5 retries)
- Add jitter to prevent thundering herd

**Fatal Errors:**
- Do NOT retry automatically
- Return error to application layer
- Log for debugging

**Backpressure Errors (4005, 6005):**
- Special case of retryable errors
- Indicate server load, not client fault
- Use longer backoff periods (start at 500ms-1s)

## Summary Checklist

Use this checklist to verify client implementation completeness:

### Connection

- [ ] AC-CONN-001: WebSocket connection
- [ ] AC-CONN-002: JWT authentication
- [ ] AC-CONN-003: Auth rejection handling
- [ ] AC-CONN-004: Anonymous mode
- [ ] AC-CONN-005: Pre-auth frame rejection
- [ ] AC-CONN-006: Rebuild client-owned state on reconnect

### KV Domain (11 criteria)

- [ ] AC-KV-001 through AC-KV-011

### Stream Domain (12 criteria)

- [ ] AC-STREAM-001 through AC-STREAM-007
- [ ] AC-STREAM-010 through AC-STREAM-014

### Queue Domain (8 criteria)

- [ ] AC-QUEUE-001 through AC-QUEUE-008

### Notice Domain (9 criteria)

- [ ] AC-NOTICE-001 through AC-NOTICE-009

### RPC Domain (8 criteria)

- [ ] AC-RPC-001 through AC-RPC-008

### Lease Domain (10 criteria)

- [ ] AC-LEASE-001 through AC-LEASE-010

### Schedule Domain (8 criteria)

- [ ] AC-SCHEDULE-001 through AC-SCHEDULE-008

### Error Handling (4 criteria)

- [ ] AC-ERROR-001 through AC-ERROR-004

### Performance (5 criteria)

- [ ] AC-PERF-001 through AC-PERF-005

**Total:** 78 explicit acceptance criteria

## Compliance Levels

### Level 1: Core Compliance (MUST)

All criteria marked as **MUST** - Required for basic Fitz client

### Level 2: Recommended Behavior (SHOULD)

All MUST + SHOULD criteria.

### Level 3: Full Compliance

All criteria including performance and edge cases

## Notes

- Criteria are written in **Given-When-Then** format for clarity
- Each criterion is independently testable
- Error codes reference CLIENT.md specification
- Timing requirements use reasonable defaults (adjust per deployment)
- Permission syntax follows format: `domain://realm/area/resource#access`
  **Last Updated:** February 3, 2026
