# Domain Actor, Data, And Admin Contracts
- Domain actor ingress mailboxes are bounded burst buffers. Each family actor uses a 16,384-message normal lane by default plus a separately bounded control lane, and the async transport ingress edge briefly retries domain dispatch when the normal lane is temporarily full. Sustained saturation still returns explicit session backpressure instead of unbounded buffering or hidden delivery guarantees. Ingress metrics separate retry attempts, frames accepted after retry, exhausted retry budgets, and wait latency.

## KV
- Actor owner: `KvDomain` is a thin mailbox adapter over a `FamilyActorPoolRuntime`; each provisioned family creates and serially owns its KV session state, locks, watches, cleanup guards, and admin projection updates.
- Persistence: committed values are durable according to the selected write policy; open transactions and watcher state are ephemeral.
- Cleanup: disconnect cleanup is enqueued on the KV family control lane, which rolls back live transactions, releases session-owned locks, and drops subscriptions without implying transaction recovery.
- `RouteFamily`/`realm`: committed rows stay partitioned by exact `RouteFamily`; `realm` remains an opaque route label and is never inferred from the family.
- Admin path: passive transaction views flow through the family-published `AdminReadModel`; exact live counts and committed value/inventory reads use family-targeted command/reply queries behind the KV admin facade.

## Queue
- Actor owner: `QueueDomain` is a thin mailbox adapter over a `FamilyActorPoolRuntime`; each family worker directly owns one `QueueFamilyState`, serializing delivery, cleanup, sweeps, admin mutations, watch state, projections, reservation state, retry bookkeeping, and durable dead-letter transitions. The family state groups warm actors and idle rotation in `QueueActorRegistry`, wildcard inventory and waiting reservations in `ReservationBook`, and maintenance deadlines in `QueueMaintenanceClock`; all three remain on the same synchronous worker.
- Reply deadline: normal client commands wait up to 60 seconds, matching the default Midge runtime response budget so an admitted storage operation is not abandoned after one second. Admission still sizes its bounded window against a one-second latency target; control and admin commands retain a one-second wait. A reply deadline after admission reports an indeterminate outcome and does not authorize an automatic retry.
- Persistence: durable backlog and dead-letter records live in storage; inflight reservations, watch subscriptions, and fast-flush state are ephemeral.
- Cleanup: disconnect cleanup is enqueued on the Queue family control lane, which clears worker reservations and watch state without implying durable ownership continuity or hidden worker recovery.
- `RouteFamily`/`realm`: queue data is isolated by exact `RouteFamily`, while `realm` remains an application-defined namespace inside the queue route.
- Admin path: live queue snapshots flow through `Runtime::queue_list_*` and the family-published `AdminReadModel`. Snapshot refresh observes runtime-owned queue state; it does not run due-work or idle sweeps. Concurrent family publications recombine the latest cached snapshots under one publication fence, so a delayed refresh cannot erase newer sibling rows. Dead-letter replay and purge use explicit family-targeted commands behind the Queue admin facade.

## Notice
- Actor owner: `NoticeDomain` dispatches through a `FamilyActorPoolRuntime`; each family worker owns its subscriptions, cleanup guards, route counters, fanout coordination, live counts, and admin snapshot updates. `NoticeRouteActor` remains a focused matching/fanout state-machine model.
- Persistence: Notice delivery, subscriptions, and counters are ephemeral only; there is no durable replay or broker-side subscriber recovery.
- Cleanup: disconnect removes session subscriptions immediately, and broker restart starts from an empty Notice state.
- `RouteFamily`/`realm`: fanout matches only within the exact `RouteFamily`; `realm` stays an opaque route segment used for filtering and admin presentation.
- Admin path: admin reads use `Runtime::notice_list_subscriptions()` and `Runtime::notice_list_routes()` backed by the passive `AdminReadModel`.

## Stream
- Actor owner: `StreamDomain` owns a `FamilyActorPoolRuntime`; every provisioned route family creates its `StreamFamilyState` on the owning worker, and both client and control commands execute serially through that family. Resource, area, and realm sequencing state is held directly by that family while committed history and recovery remain `StreamStore` authoritative.
- Current runtime boundary: `StreamDomain` is the crate-private delivery adapter for client Stream frames and family-targeted control/admin commands.
- Reply deadline: normal client commands wait up to four seconds for the family actor so queued synchronous disk commits can finish; control and admin commands retain a one-second wait. A deadline after admission reports an indeterminate outcome and does not authorize an automatic COMMIT retry.
- Persistence: committed records, metadata, and watermarks are durable; live append sessions and subscriptions are ephemeral.
- Cleanup: disconnect aborts append sessions and drops live subscriptions without restoring them on reconnect.
- `RouteFamily`/`realm`: committed history is partitioned by exact `RouteFamily`, while realm and area indexes stay explicit storage keys rather than family aliases.
- Admin path: a normal admin refresh sends projection requests only to provisioned families whose shared dirty flag is set, then waits for those replies. Clean families retain their last published snapshot without an actor round trip; a stopped clean family is still detected and has its ephemeral active-session observation cleared. Workers on separate shards can progress independently; families sharing a shard remain serial. Each refreshed worker projects its own committed rows, watermarks, and live-session overlay. When refreshed, the first worker also includes persisted but unprovisioned families in admin-visible history, without creating live sessions for them. A shared coordinator merges the latest successful worker snapshots without letting one refresh erase another's rows. A failed worker retains its last published committed observation but clears its ephemeral active-session counts while healthy workers can refresh. The three unlabeled live gauges aggregate the latest counts published by each family; after a failed family is observed during admin refresh, its live contribution is dropped. Read-model projections and watermark views flow through `Runtime::stream_list_*`; committed record inspection uses `Runtime::stream_read_resource_records()`. Raw metrics scrapes remain passive and do not trigger projection work.

## RPC
- Actor owner: `RpcDomain` dispatches normal and control work through a `FamilyActorPoolRuntime`; each family worker serializes registrations, pending calls, response assembly, timeout sweeps, cleanup, live counts, and admin snapshot updates.
- Current runtime boundary: RPC commands operate only on the `RpcFamilyState` supplied to the owning family worker. Per-concrete-route dispatch and fairness state is removed once that route has no queued or pending call, even while a wildcard registration remains live. Broker-wide admission remains an explicit shared capacity component.
- Persistence: worker registrations, pending calls, and reply assembly are ephemeral; RPC does not provide restart-safe backlog durability.
- Cleanup: disconnect unregisters workers, expires pending session state, and never restores inflight calls or subscriptions.
- `RouteFamily`/`realm`: dispatch and replies stay within the exact `RouteFamily`; `realm` remains an application-defined route component for operation naming and filters.
- Admin path: worker and pending-call views flow through `Runtime::rpc_list_workers()` and `Runtime::rpc_list_pending()` backed by the read model.
- Deadline/cancellation boundary: [RPC deadlines and cancellation](rpc-cancellation-lifecycle.md) specifies capability negotiation, budget propagation, acknowledgment, worker credit, grace expiry, and legacy fallback.

## Lease
- Actor owner: `LeaseDomain` dispatches through a `FamilyActorPoolRuntime`; each family worker serializes delivery, cleanup, expiry sweeps, ownership, waiters, subscriptions, and fencing-token progression inside one running broker.
- Current runtime boundary: Lease commands execute against the `LeaseFamilyState` created for the owning family. The shared list-snapshot coordinator bounds broker-wide observation memory without owning lease mechanics.
- Persistence: leases, waiters, and fencing tokens are ephemeral broker-local coordination state only; there is no durable lease history or restart recovery.
- Cleanup: disconnect releases session-owned leases, clears waiters, and never implies cross-restart ownership continuity.
- `RouteFamily`/`realm`: lease coordination is isolated by exact `RouteFamily`; `realm` stays an opaque application namespace carried by the route, not a family synonym.
- Admin path: lease snapshots flow through `AdminReadModel`; exact live counts and waiter inspection use family-targeted command/reply reads behind the lease admin facade.

## Schedule
- Actor owner: `ScheduleDomain` dispatches through a `FamilyActorPoolRuntime`; each family worker owns its `ScheduleFamilyState` definitions projection, watches, pending acknowledgement retries, execution counters, cleanup state, due scans, and admin snapshot refresh.
- Current runtime boundary: client and control commands execute against the state created for their owning family, while durable claims remain authoritative in `ScheduleStore`.
- Current scaling ceiling: work within one route family is deliberately serialized. A priority due-scan performs synchronous claim and acknowledgement commits in batches capped at 32 pending fires for that family, so strict cloud durability or degraded provider latency can delay Create, List, and Cancel traffic in the same family without blocking sibling families.
- Persistence: schedule definitions, next-fire state, and pending claims are durable timing intent; subscriber watches and transient handoff coordination are ephemeral.
- Cleanup: disconnect removes live watches but does not erase persisted schedule intent or imply replay of every missed interval after downtime.
- Delivery boundary: `broadcast` attempts every matching live registration; `single` uses registration order and a per-concrete-route ephemeral round-robin cursor until one router handoff succeeds. A cursor is discarded when no live registration still matches its concrete route. Strict `*` and `**` registration patterns never cross RouteFamily boundaries. Zero accepted handoffs still acknowledge the pending claim and advance, because Schedule owns timing rather than durable consumer availability.
- `RouteFamily`/`realm`: schedules stay partitioned by exact `RouteFamily`, while `realm` remains an application-defined route label that is never derived from the family.
- Admin path: schedule projections flow through the passive read model after family-owned snapshot publication; exact counters and pending-claim inspection use family-targeted command/reply reads behind the schedule admin facade.
