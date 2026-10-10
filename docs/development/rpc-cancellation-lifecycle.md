# RPC deadlines and cancellation

This document specifies the negotiated RPC lifecycle required by
[#358](https://github.com/cntryl/fitz/issues/358). RPC remains live,
process-local coordination: a broker restart drops worker registrations and
pending calls.

## Capability and wire format

The broker advertises `CAP_RPC_CANCELLATION` (`1 << 3`) in `SERVER_HELLO`.
Clients that do not observe that capability keep sending the legacy payloads.
The capability permits these optional request extensions and lifecycle
controls:

| Message | Payload after the legacy fields | Meaning |
| --- | --- | --- |
| `SUBSCRIBE` 300 | `[version:u8=1, flags:u8]`, bit 0 set | Worker accepts cancellation controls and remaining budgets |
| `REQUEST` 302 | `[version:u8=1, flags:u8=1, remaining_budget_ms:u32]` | Caller budget at client send time, rounded down to milliseconds |
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

Request and registration extensions are optional and follow the legacy fields
without changing their encoding. The broker accepts budgets from 0 through
86,400,000 ms; larger values, unknown extension versions, unsupported flags,
truncated extensions, and trailing extension bytes are rejected. A legacy
worker receives the original request bytes, without an extension. `RPC_CANCEL`
is session-owned and tied to the caller's route family and session. A cleanup
acknowledgment is accepted only from the worker session that owns that live
call and only when that worker negotiated support.

Caller and worker correlation IDs have distinct roles. For each dispatch to a
supporting worker, the broker replaces the request's correlation UUID with a
fresh opaque invocation UUID. The worker echoes that delivered UUID in every
response and cleanup acknowledgment; worker cancellation signals carry it too.
The broker validates the owning worker session and route family, then restores
the caller's UUID in caller-facing responses. The caller keeps using its original
UUID for cancellation. Legacy workers receive the original UUID unchanged.

The invocation index contains only live dispatched calls, including canceled
calls awaiting cleanup, and is removed with the pending call. Reusing a caller
UUID therefore cannot let a delayed or duplicate acknowledgment, progress chunk,
or terminal response from an earlier invocation release or complete a newer
call. A reconnect creates a new worker session and cannot inherit this identity.

## Deadlines

API ingress stamps each request with a local monotonic receive time before it
enters the family actor. The stamp is taken once when ingress starts
dispatching the frame, and ingress backpressure retries reuse it; time the
frame waited in the session's transport queue before that is not counted. The actor subtracts time spent in its queue from the
remaining budget. It caps that budget to the configured broker request timeout
(30 seconds by default) and stores the resulting monotonic deadline with the
existing queued or pending request state. Queueing never renews the deadline.
An already-expired request is rejected before worker credit or global pending
capacity is claimed. A queued request that expires before dispatch gets one
terminal timeout and releases its reservations.

For a worker that negotiated support, the broker forwards the remaining
budget on `REQUEST` 302. The worker's local monotonic timer is authoritative
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

An absent inbound budget remains absent. When present, the SDK computes the
current remaining time at dispatch and carries it forward; cancellation of the
parent handler asks the downstream broker to cancel as well.

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
it. Invalid settings log a warning and use the five-second default. If a
supporting worker has not acknowledged cleanup by expiry, the
broker requests transport-level session close. Credit remains reserved until
actual session cleanup removes that worker's pending calls. Failed close
requests are retried every 250 ms. Grace expiry is a bound on broker cleanup,
not proof that remote code or side effects have stopped.

Cancellation is never an automatic retry signal. Applications must decide
whether a call can safely be repeated. Workers retain legacy behavior when the
capability is absent, and callers receive no stronger cancellation guarantee
than the negotiated worker contract provides.
