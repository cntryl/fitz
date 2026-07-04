
### AC-SCHEDULE-001: Create Scheduled Job

**MUST** create job with cron expression
**Given:** Session with `schedule://realm/area/**#write` permission  
**When:** Client sends `Create(route="schedule://prod/jobs/backup", cron="0 2 * * *", payload="backup-db")`  
**Then:**

- Server returns `CreateOk(job_id)`
- Job is persisted
- Job will trigger at 2:00 AM daily

### AC-SCHEDULE-002: Cron Expression Validation

**MUST** reject invalid cron expressions
**Given:** Client attempts to create job  
**When:** Client sends `Create(cron="invalid syntax")`  
**Then:**

- Server returns error code `7002` (Invalid Cron)
- Job is NOT created

### AC-SCHEDULE-003: Job Execution Notification via SCHEDULE_SUBSCRIBE / SCHEDULE_NOTIFY

**MUST** receive notification when schedule fires
**Given:**

- Job created with cron `"*/1 * * * *"` (every minute) on route `schedule://prod/app/reminders`
- Client sends `SCHEDULE_SUBSCRIBE` (703) to `schedule://prod/app/reminders`
  **When:** Time advances to next minute boundary  
  **Then:**
- Client receives `SCHEDULE_NOTIFY` (705) with the job's configured payload
- The broker also executes the schedule's `target_resource` via the `DomainPublishEvent` system
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

- Server returns error code `7009` (ERR_UNAUTHORIZED)
- Job NOT created

## Error Handling

### AC-ERROR-001: TLV Parse Errors

**MUST** handle malformed TLV frames gracefully
**Given:** Client sends invalid TLV (incorrect length field)  
**When:** Server receives malformed frame  
**Then:**

- Server closes connection with parse error
- Client logs error and does NOT retry same malformed data
- **Duplicate TLV tags are NOT permitted.** If a TLV tag appears more than once the frame **MUST** be treated as malformed and the server **MUST** close the connection with a parse error. **Rationale:** Disallowing duplicate tags keeps decoding deterministic and simplifies client implementations. Clients **MUST NOT** send duplicate tags.

### AC-ERROR-002: Domain Error Codes

**MUST** correctly parse domain-specific error codes
**Given:** Client sends unauthorized operation  
**When:** Server returns error with domain-specific code (e.g., `4009` for Queue)  
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

- `1001` (Transaction Not Found) → Fatal, do NOT retry
- `6001` (ERR_RPC_TIMEOUT; worker accepted but did not reply before timeout) → Retryable with backoff
- `6004` (ERR_ROUTE_NOT_REGISTERED; no workers registered for route) → Retryable with backoff
- `1011` (KV Unauthorized) → Fatal, do NOT retry
- `2009` (Stream Unauthorized) → Fatal, do NOT retry
- `4009` (Queue Unauthorized) → Fatal, do NOT retry
- `5009` (Lease Unauthorized) → Fatal, do NOT retry
- `6009` (RPC Unauthorized) → Fatal, do NOT retry
- `7009` (Schedule Unauthorized) → Fatal, do NOT retry
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

**MUST** respect maximum frame size (default 1 MB production, configurable)
**Given:** Client attempts to send large payload  
**When:** Payload exceeds configured limit (1 MB production default)  
**Then:**

- Client either:
  - Rejects operation before sending, OR
  - Server rejects with frame size error
- Client chunks large data across multiple frames/operations
- **A single TLV value MUST NOT exceed 65535 bytes (≈64 KiB).** Large payloads **MUST** be chunked across multiple frames or operations; clients and servers **MUST NOT** rely on a single TLV value larger than 65535 bytes even when the frame size permits it.
  **Chunking notes:**
- **RPC** supports explicit chunked responses (see AC-RPC-005).
- **Stream** responses MAY be split across multiple frames or partial records.
- Other domains (e.g., KV, Queue) should use multiple logical operations or application-level chunking; clients MUST NOT rely on implicit TLV chunk reassembly in those domains.
  **Configuration:**
- Server default: 1 MB (configurable via `BootConfig::max_frame_size`)
- Client SDK default: May be higher (e.g., 100 MB) but should be reduced to match server in production
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
| 1001 | ERR_TRANSACTION_NOT_FOUND | Transaction ID does not exist or expired | No |
| 1002 | ERR_INVALID_MODE | Invalid transaction mode specified | No |
| 1003 | ERR_KEY_NOT_FOUND | Key does not exist in transaction view | No |
| 1004 | ERR_ISOLATION_CONFLICT | Read-write set conflict detected | Yes (with backoff) |
| 1005 | ERR_WRITE_IN_READONLY | Write attempted in read-only transaction | No |
| 1006 | ERR_KEY_EXISTS | Key already exists (Insert failed) | No |
| 1007 | ERR_INVALID_ROUTE | Route format invalid or malformed | No |
| 1008 | ERR_REALM_MISMATCH | Operation crosses realm boundaries | No |
| 1009 | ERR_BACKEND_ERROR | Storage backend error | Yes (with backoff) |
| 1010 | ERR_TRANSACTION_ABORTED | Transaction aborted by system | No |
| 1011 | ERR_UNAUTHORIZED | Permission denied for KV operation | No |

### Stream Domain (2000-2999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 2001 | ERR_CONCURRENCY_CONFLICT | Expected offset does not match (AC-STREAM-013) | No |
| 2002 | ERR_SESSION_ALREADY_ACTIVE | Another append session is already active for that resource | No |
| 2003 | ERR_SESSION_NOT_FOUND | Session ID is missing, stale, or already cleaned up | No |
| 2004 | ERR_INVALID_READ_BOUND | Read range bounds invalid | No |
| 2005 | ERR_RESOURCE_NOT_FOUND | Stream resource does not exist | No |
| 2006 | ERR_STREAM_FILTER_UNSUPPORTED_VERSION | Filter marker/version is not supported by this broker | No |
| 2007 | ERR_STREAM_FILTER_INVALID_PAYLOAD | Filter payload malformed or undecodable | No |
| 2009 | ERR_UNAUTHORIZED | Permission denied for stream operation | No |
| 2010 | ERR_INVALID_SUBSCRIPTION_PATTERN | Subscription pattern syntax invalid | No |
| 2011 | ERR_SUBSCRIPTION_LIMIT | Maximum subscriptions reached | No |

### Notice Domain (3000-3999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 3001 | ERR_INVALID_ROUTE | Notice route format invalid | No |
| 3002 | ERR_INVALID_PATTERN | Subscription pattern syntax invalid | No |
| 3003 | ERR_SUBSCRIPTION_LIMIT | Maximum subscriptions reached | No |
| 3004 | ERR_TRANSPORT_CLOSED | Transport connection closed | No |
| 3009 | ERR_UNAUTHORIZED | Permission denied for notice operation | No |

### Queue Domain (4000-4999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 4001 | ERR_INVALID_TOKEN | Queue inflight token invalid or wrong (AC-QUEUE-006) | No |
| 4002 | ERR_INFLIGHT_EXPIRED | Message inflight reservation expired | No |
| 4003 | ERR_MESSAGE_NOT_FOUND | Message ID not found in queue | No |
| 4004 | ERR_QUEUE_NOT_FOUND | Queue resource does not exist | No |
| 4005 | ERR_QUEUE_FULL | Queue at capacity (backpressure) | Yes (with backoff) |
| 4009 | ERR_UNAUTHORIZED | Permission denied for queue operation | No |

### Lease Domain (5000-5999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 5001 | ERR_LEASE_HELD | Lease already held by another client | Yes (with backoff) |
| 5002 | ERR_INVALID_FENCE | Fencing token invalid or out of order | No |
| 5003 | ERR_LEASE_EXPIRED | Lease TTL expired | No |
| 5004 | ERR_LEASE_NOT_FOUND | Lease resource does not exist | No |
| 5005 | ERR_INVALID_TOKEN | Lease token invalid or wrong (AC-LEASE-009) | No |
| 5009 | ERR_UNAUTHORIZED | Permission denied for lease operation | No |

### RPC Domain (6000-6999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 6001 | ERR_RPC_TIMEOUT | No response within timeout period | Yes (with backoff) |
| 6002 | ERR_WORKER_NOT_FOUND | Worker disconnected or unregistered | Yes |
| 6003 | ERR_RPC_BACKPRESSURE | RPC queue at capacity (backpressure) | Yes (with backoff) |
| 6004 | ERR_ROUTE_NOT_REGISTERED | No workers registered for route (AC-RPC-003) | Yes (with backoff) |
| 6005 | ERR_CORRELATION_NOT_FOUND | Correlation ID not found (orphaned response) | No |
| 6009 | ERR_UNAUTHORIZED | Permission denied for RPC operation | No |

### Schedule Domain (7000-7999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 7001 | ERR_SCHEDULE_NOT_FOUND | Schedule job does not exist | No |
| 7002 | ERR_INVALID_CRON | Cron expression syntax invalid (AC-SCHEDULE-002) | No |
| 7003 | ERR_SCHEDULE_LIMIT | Maximum schedules reached | No |
| 7004 | ERR_PARSE_ERROR | Schedule payload parse error | No |
| 7005 | ERR_INVALID_TARGET | Target route invalid or unsupported | No |
| 7006 | ERR_INVALID_SUBSCRIPTION_PATTERN | Subscription pattern syntax invalid | No |
| 7007 | ERR_SUBSCRIPTION_LIMIT | Maximum subscriptions reached | No |
| 7009 | ERR_UNAUTHORIZED | Permission denied for schedule operation | No |

### Error Handling Guidelines

**Retryable Errors:**
- Implement exponential backoff (e.g., 100ms, 200ms, 400ms, 800ms, max 5s)
- Limit retry attempts (e.g., max 5 retries)
- Add jitter to prevent thundering herd

**Fatal Errors:**
- Do NOT retry automatically
- Return error to application layer
- Log for debugging

**Backpressure Errors (4005, 6003):**
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

### Level 2: Production Ready (SHOULD)

All MUST + SHOULD criteria - Recommended for production deployments

### Level 3: Full Compliance

All criteria including performance and edge cases

## Notes

- Criteria are written in **Given-When-Then** format for clarity
- Each criterion is independently testable
- Error codes reference CLIENT.md specification
- Timing requirements use reasonable defaults (adjust per deployment)
- Permission syntax follows format: `domain://realm/area/resource#access`
  **Last Updated:** February 3, 2026
