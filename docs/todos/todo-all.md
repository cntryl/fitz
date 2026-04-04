# Fitz Domain Contracts

This directory is the canonical contract surface for Fitz domain semantics. These files are not feature wishlists. They define the boundaries, guarantees, invariants, anti-goals, and required proof points for the current server.

## Source Of Truth

- Current implementation and current tests outrank historical todo prose and narrative docs.
- Current admin and metrics surfaces are part of the contract because operators depend on them.
- If behavior is not proven in code or tests, do not promote it into a guarantee. Record it as a required test or wording gap.
- Ambiguity is a defect. When a domain boundary is fuzzy, this directory must resolve it explicitly.

## Shared Terms

- Committed: persisted according to the domain's configured storage write policy.
- Durable: survives broker restart when the write policy says the data is committed.
- Ephemeral: current-process or current-session state that disappears on disconnect or restart.
- Session-scoped: tied to the live connection context; disconnect destroys it.
- Best-effort: Fitz attempts the operation but does not promise recovery, retry, or durable completion of downstream effects.
- Ordered: the contract defines a stable order the client can rely on.
- Replayable: the domain exposes a supported way to read committed history again.
- Recovery: rebuilding correct client state after disconnect or restart.
- Recovery owner: the side that must drive rebuild. In Fitz today this is almost always the client unless the domain explicitly persists committed state.

## System Rules

- Sessions are ephemeral across the entire system. The broker never restores session state automatically after disconnect.
- RouteFamily is a hard isolation boundary. Cross-family delivery, recovery, or state bleed is a contract violation.
- Notice is the live-now fanout domain. It is not a recovery, replay, or history domain.
- Stream is the durable history, replay, and catch-up domain. If a client needs rebuild, catch-up, or historical replay, it belongs to Stream.
- KV, Queue, Schedule, and Stream may persist committed state. They still keep some live coordination state in memory.
- Lease, Notice, and RPC are intentionally ephemeral current-process facilities.
- RouteFamily selection may remain a deployment concern for now. Durable behavior must not depend on server-side recovery of broker-local session memory.

## Canonical Files

- [todo-notice.md](todo-notice.md)
- [todo-stream.md](todo-stream.md)
- [todo-kv.md](todo-kv.md)
- [todo-queue.md](todo-queue.md)
- [todo-rpc.md](todo-rpc.md)
- [todo-lease.md](todo-lease.md)
- [todo-schedule.md](todo-schedule.md)

## Domain Matrix

| Domain | Purpose | Committed Durable State | Live State Lost On Disconnect Or Restart | Ordered Contract | Replayable | Recovery Owner |
| --- | --- | --- | --- | --- | --- | --- |
| Notice | Live fanout to current subscribers | None | All subscriptions and delivery state | Match correctness only; no durable delivery order | No | Client |
| Stream | Durable append and replay | Committed records, counters, watermarks | Append sessions and subscriptions | Yes, by resource and commit-time higher scopes | Yes | Client uses durable offsets |
| KV | Committed key/value state | Committed values | Open transactions and locks | Not a history order surface | No | Mixed: broker for committed state, client for transactions |
| Queue | Durable work backlog | Committed messages and indexes | Live leases, tokens, warm actors | Ready-queue order with competing-consumer limits | DLQ replay only, not stream replay | Mixed |
| RPC | Live request/response dispatch | None | Workers, pending requests, reply routing | FIFO pending queue per route, plus strict streaming sequence | No | Client and worker |
| Lease | Single-process coordination | None | All lease ownership and waiters | Wait ordering only within the local actor contract | No | Client |
| Schedule | Durable timing definitions | Schedule definitions, pending fire claims | Live subscriptions | Due-time ordering inside one schedule actor, no historical replay of missed runs | Definitions survive; missed runs are skipped | Broker for persisted definitions, client for live subscriptions |

## Composition Rules

- Use Notice when the consumer only cares about live delivery while connected.
- Use Stream when the consumer must recover, rebuild, backfill, or inspect history.
- Use Queue when work must be reserved, acknowledged, retried, or dead-lettered.
- Use RPC when the caller needs a live worker response, not durable backlog.
- Use Lease when a workflow needs local single-broker ownership or fencing, not durable lock recovery.
- Use Schedule when the server must remember future execution intent across restart.
- Use KV for current authoritative state. Use Stream for historical record. Do not make one silently impersonate the other.
- Use Queue with Lease only when the client wants explicit cross-resource workflow coordination. Queue visibility alone is not a global ownership system.

## Ambiguity Watch List

- Notice versus Stream recovery expectations. Fitz must never imply that missed Notice traffic can be rebuilt unless the producer also wrote the event to Stream.
- Queue versus Stream consumption semantics. Queue is for mutable work state; Stream is for immutable history.
- RPC streaming versus Stream replay. RPC sequence numbers are live response assembly, not durable history cursors.
- Lease ownership versus Queue visibility. A queue lease does not become a durable lease-domain token.
- Schedule delivery versus durable schedule state. Persisted schedules survive; schedule notifications remain live-only delivery.

## Repo-Wide Wording Rule

All broader docs should use the same crisp wording as this directory:

- Notice is live-only and ephemeral.
- Stream is the durable replay and backfill surface.
- Sessions are ephemeral.
- Broker-side recovery exists only for explicitly persisted committed state.
- Client rebuild is explicit, not magical.