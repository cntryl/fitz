# RPC deadlines and cancellation

This document specifies the negotiated RPC lifecycle required by
[#358](https://github.com/cntryl/fitz/issues/358). RPC remains live,
process-local coordination: a broker restart drops worker registrations and
pending calls.

## Wire format

Cancellation controls and remaining budgets are base protocol in wire
format 2; nothing is negotiated. The fields and lifecycle controls are:

| Message | Payload after the route, credit, or body | Meaning |
| --- | --- | --- |
| `SUBSCRIBE` 300 | `[flags:u8]`, bit 0 set | Worker accepts cancellation controls and remaining budgets |
| `REQUEST` 302 (caller) | `[has_budget:u8][remaining_budget_ms:u32]?` | Caller budget at client send time, rounded down to milliseconds; absent means the broker default |
| `REQUEST` 302 (delivery) | `[1:u8][remaining_budget_ms:u32]` | The broker's remaining budget for the call, on every delivery |
| `RPC_CANCEL` 304, kind 1 | `[kind:u8=1, correlation_id:uuid16, reason:u8]` | Caller asks the broker to cancel; reason 1 is explicit, 2 is caller deadline |
| `RPC_CANCEL` 304, kind 3 | `[kind:u8=3, correlation_id:uuid16]` | Worker confirms its handler and request cleanup have finished |
| `RPC_LIFECYCLE` 305, kind 2 | `[kind:u8=2, correlation_id:uuid16, reason:u8]` | Broker signals a worker; reasons 1 explicit, 2 caller deadline, 3 caller disconnected, 4 broker deadline |
| `RPC_LIFECYCLE` 305, kind 4 | `[kind:u8=4, correlation_id:uuid16, status:u8]` | Broker reports caller cancellation handling |

Caller cancellation statuses are 1 queued request removed, 2 cancellation
signal routed, 3 worker does not support cancellation, 4 already canceled and
awaiting worker cleanup, 5 unknown or unauthorized request, and 6 signal forwarding failed. Status 2
means the broker routed the signal. It does not mean the worker stopped.
Removed or normally completed calls also return status 5: the broker retains
no completion tombstones, so a late control cannot reveal another caller's
completed state. Malformed control frames are discarded and counted without
mutating pending calls; a caller receives no successful cancellation result.

The broker accepts budgets from 0 through 86,400,000 ms; larger values,
reserved registration flag bits, an unknown presence byte, truncated fields,
and trailing bytes are rejected. A worker that did not set bit 0 still
receives the remaining budget on every delivery; it is not sent cancellation
controls. `RPC_CANCEL`
is session-owned and tied to the caller's route family and session. A cleanup
acknowledgment is accepted only from the worker session that owns that live
call and only when that worker negotiated support.

Caller and worker correlation IDs have distinct roles. For each dispatch to a
supporting worker, the broker replaces the request's correlation UUID with a
fresh opaque invocation UUID. The worker echoes that delivered UUID in every
response and cleanup acknowledgment; worker cancellation signals carry it too.
The broker validates the owning worker session and route family, then restores
the caller's UUID in caller-facing responses. The caller keeps using its original
UUID for cancellation. A worker that did not declare cancellation support
receives the original UUID unchanged.

The invocation index contains only live dispatched calls, including canceled
calls awaiting cleanup, and is removed with the pending call. Reusing a caller
UUID therefore cannot let a delayed or duplicate acknowledgment, progress chunk,
or terminal response from an earlier invocation release or complete a newer
call. A reconnect creates a new worker session and cannot inherit this identity.

## Deadlines

The transport stamps each complete frame with a local monotonic receive time.
TCP captures it immediately after the socket read that completes the frame,
before extracting or forwarding that read's batch; all frames completed by
that read share the stamp. WebSocket captures it when the stream yields a
binary message, before validation and session processing. This starts the
broker's clock after transport receipt; it does not measure client-to-broker
network transit or bytes still buffered before that receipt.

Each frame contains exactly one complete message, which keeps that receive
stamp throughout processing. Session handoff queueing, authentication, authorization,
ingress backpressure retries, and the family actor mailbox all consume the
same RPC deadline. The mailbox admission retry budget starts separately when
dispatch begins, so old transport frames do not prematurely exhaust another
domain's admission retries. Direct callers of the compatible unstamped
ingress/session APIs start their clock when those APIs are entered.

The actor subtracts elapsed receive time from the remaining budget. It caps
that budget to the configured broker request timeout (30 seconds by default)
and stores the resulting monotonic deadline with the
existing queued or pending request state. Queueing never renews the deadline.
An already-expired request is rejected before worker credit or global pending
capacity is claimed. A queued request that expires before dispatch gets one
terminal timeout and releases its reservations.

The broker forwards the remaining budget on every `REQUEST` 302 delivery.
The worker's local monotonic timer is authoritative
for its own handler; any downstream RPC must receive the remaining budget and
an explicit parent cancellation link. Network delay makes propagated
cancellation best effort. A deadline cannot undo application side effects.

## Explicit downstream links

Each SDK passes the inbound handler context into the downstream call directly;
the broker never infers a parent call from tracing or other metadata. The
equivalent APIs are:

| SDK | Downstream call |
| --- | --- |
| Rust | `rpc.call_from_request(&request, route, body).await?` |
| Python | `rpc.call_from_request(request, route, body)` |
| Go | `client.RPC().Call(ctx, route, body)` |
| TypeScript | `rpc.call(route, { body, timeoutMs: context.remainingTimeMs(), signal: context.signal })` |
| .NET | `rpc.CallAsync(route, body, request.RemainingTime, ct)` |

Every inbound request carries a budget. The SDK computes the current
remaining time at dispatch and carries it forward; cancellation of the parent
handler asks the downstream broker to cancel as well.

## Cancellation and capacity

The route-family actor orders request, cancel, response, acknowledgment, and
session-cleanup events. For a queued call, cancellation removes that call and
releases its pending reservation. For a dispatched call on a supporting worker,
the broker detaches the caller, routes the cancellation signal, and retains
worker credit and global pending capacity until one of these events occurs:

1. The owning worker acknowledges that handler and request cleanup are done.
2. Session cleanup runs after the broker closes the worker or the worker
   disconnects.

These paths are actor-serialized and remove the pending entry once. Progress
and terminal responses after cancellation are ignored and cannot release
execution credit before handler cleanup. Supporting SDK workers send cleanup
acknowledgments after every invocation finishes cleanup, even when a delayed
cancellation notification has not yet arrived. The broker accepts the owning
worker acknowledgment only for an already canceled live call; normal completed
calls and duplicate acknowledgments are ignored. If the
worker unregisters its route, canceled execution still retains its pending
reservation until acknowledgment or session cleanup. Unregistration stops new
dispatch and does not prove that an existing handler has stopped. If the
worker has no cancellation support, an explicit cancel returns status 3 and
leaves the legacy request lifecycle intact. When a caller disconnects, the
broker sends best-effort cancellation only to workers that negotiated support.

The cancellation grace defaults to 5,000 ms. Set
`FITZ_RPC_CANCELLATION_GRACE_MS` to a value from 0 through 86,400,000 to change
it. Invalid settings log a warning and use the five-second default.

A session reads and processes inbound frames one at a time, so a worker's
cleanup acknowledgment can wait behind a slow frame on the same connection,
such as a Stream commit waiting on storage. The grace therefore runs only while
the worker session's frame loop is free to read the acknowledgment: the TCP or
WebSocket transport records how long the loop spends processing frames, and
when the grace deadline passes the broker pushes it back by any busy time since
the grace started or was last extended. Busy time can defer the close request
up to the fixed ceiling below. Busy time is read from the live session inbox at
the timeout sweep, so
the deadline can be deferred repeatedly while one long frame stays in progress.
Total deferral per cancellation is capped at 60 seconds
(`RPC_MAX_CANCELLATION_GRACE_DEFERRAL`), matching the Stream client storage
reply budget. The grace deadline is never extended beyond the nominal grace
expiry plus 60 seconds, even if its frame loop never frees up or it never
acknowledges. The ceiling is fixed and does not
change with `FITZ_RPC_CANCELLATION_GRACE_MS`.
When the next timeout sweep observes expiry with no deferral available and the
worker has not acknowledged cleanup, the broker requests transport-level
session close. Credit remains reserved until
actual session cleanup removes that worker's pending calls. Failed close
requests are retried every 250 ms. Grace expiry controls when the broker
requests cleanup; actual session cleanup depends on transport finalization.
Remote code and side effects require independent verification.

Cancellation is never an automatic retry signal. Applications must decide
whether a call can safely be repeated. Workers retain legacy behavior when the
capability is absent, and callers receive no stronger cancellation guarantee
than the negotiated worker contract provides.
