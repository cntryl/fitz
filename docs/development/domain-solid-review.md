# Domain SOLID evidence review

Scope: the seven domains and their shared runtime, subscription, time, storage
and dispatch boundaries. Tracking issue: [#499](https://github.com/cntryl/fitz/issues/499).
This review accompanies the Schedule ownership and cron-invariant changes on
`feature/0.3.0-wire-format`.

The acceptance target is complete coverage of the criteria below and no open
material findings in this scope. SOLID is a design judgment, not a numerical
property proved by test coverage or a model probability. This review does not
qualify a future Midge version, storage-provider recovery, SDK conformance,
deployment, or changes made after the reviewed source.

## Method and closure rules

- Inspect actual state owners, mutation paths, interfaces, implementations,
  callers, composition roots and applicable behavior tests.
- For single responsibility, identify independent reasons to change and the
  owner that protects each invariant. A smaller source file is not enough.
- For open/closed, trace a bounded representative change through current
  ownership. An intentional wire/storage format change legitimately touches
  its codecs and compatibility documentation. Closed enums are not evidence
  that every domain needs a plugin mechanism.
- For substitution, use each actual trait's guarantees, errors and capability
  limits. Different domains do not promise identical durability or delivery.
- For interface segregation, inspect operations required by real callers.
  Importing one Rust type does not require using every item in its module.
- For dependency inversion, keep policy separated from infrastructure through
  meaningful adapters and roles. The intentional single Midge backend does
  not require a generic repository trait for every store.
- Submit complexity tradeoffs and consequential design choices to Jev with
  the actual code, callers, constraints and explicit alternatives. Verify its
  recommendation against source and focused native evidence before adopting it.
- A row closes as satisfied, remediated, or not applicable with its reason.
  Unknown evidence is not a pass. The source decisions below are checked by
  the existing behavioral suites and the new cron contract tests.

Jev 1.14.0 received numbered current-source excerpts, interface contracts,
callers, bounded change traces and existing test names in seven independent
domain requests. Each request asked separately about S, O, L, I and D, with
explicit material-finding and missing-evidence alternatives. All 35 selected
no material finding in the supplied boundary. The initial substitution choices
were relatively diffuse; their closure also requires the concrete shared and
per-domain evidence below. Model selections are advisory, not closure proof.
The decision and validation record is maintained in #499.

A narrower follow-up supplied the seven actual cleanup implementations,
mailbox entry points, stale-work gates and observed native contract results.
It selected no supported common-contract conflict for every domain. The
source-backed decisions below and executable tests remain the closure evidence;
these probabilities are not combined into a certainty score.

A final patch challenge supplied the new Schedule owners, actor callers,
public exports, native regression results and migration text. It selected no
supported material finding for the cron invariant, ownership direction or
documented Rust source breaks. Those selections do not replace the source
checks or native tests.

The explicit complexity review compared immutable compiled cron state with
setters or an evaluator redesign; focused private owners with consolidation or
public API expansion; and an internal actor cache with a supported public cache
or an additional transfer model. Jev selected the current immutable state,
private owners with compatibility paths, and internal cache with an explicit
source break. These choices repair the concrete invariant and ownership issue
without changing evaluation algorithms or adding adapter layers.

## State, dependency and caller inventory

| Domain | Authoritative live owner | Infrastructure and read surfaces |
| --- | --- | --- |
| Notice | `sink/state.rs`, subscriptions and bounded delivery jobs | Router/delivery worker; passive admin projection; no store |
| Stream | `actor.rs` append session; store owns committed offsets/history | `store/` and `storage/` wrap Midge; scope codecs; sink coordinates live notifications |
| KV | `actor/mod.rs::ActiveKvTx` and sink resource locks | `store.rs` is the transaction/error adapter; inventory and admin are separate |
| Queue | `actor/mod.rs` ready/inflight/delayed state; reservation book owns parked callers | `actor/recovery_store.rs::QueuePersistence` / `QueueStore`; sink admission and passive projection |
| RPC | `sink/state_model/` pending, worker registration and credit tables | Registration/dispatch/response role traits; response forwarder; no durable store |
| Lease | `sink/model.rs` family ownership, fencing and waiters | Separate acquisition, expiry, list snapshot and cleanup paths; no durable store |
| Schedule | `actor/model.rs` definition cache, heap and claims | Private cron/route owners; `SchedulePersistence` / `store/`; pure recipient selection; protocol DTOs |

`src/boot/domains.rs` is the concrete composition root: it chooses storage,
policies and endpoints, registers them as `MailboxSink`, and produces narrow
maintenance/admin views. `src/boot/domain_interfaces.rs` gives maintenance
callers the operations they need without exposing every domain operation.
Actual message encoding stays in `src/protocol/*_codec.rs`.

Public request/response/identity values are transfer data. Their fields do not
mutate actor-owned state or hidden compiled representations. Domain dispatch
validates scope, session, route family and ownership before mutation. The cron
type was an exception to that rule and is corrected in this change.

## The 35 domain/principle decisions

Paths in this section are relative to `src/domains/` unless stated otherwise.
Test names identify executable behavior evidence, not source-layout assertions.

### Notice

| Principle | Status | Source/caller decision and evidence |
| --- | --- | --- |
| S | satisfied | `notice/sink/publish.rs`, `delivery_worker.rs`, subscriptions and `cleanup.rs` separate acceptance, live delivery and session release. `should_accept_notice_publish_without_waiting_for_subscriber_delivery` and `should_keep_notice_family_responsive_while_delivery_worker_is_blocked` distinguish these contracts. |
| O | satisfied | A route-validation/error change belongs to validation and the typed error/codec boundary, not fanout scheduling. `protocol.rs::NoticeClientError` centralizes the legacy string conversion; `should_classify_real_pattern_compiler_errors_as_invalid_patterns` covers the boundary. |
| L | satisfied | `MailboxSink` and `SessionScoped` do not promise durable fanout. `should_reject_notice_delivery_when_family_runtime_is_stopped` and `should_reject_stale_subscribe_after_disconnect_cleanup_marks_session` exercise failure and cleanup. Shared subscription identity uses `NoticeSubscription`. |
| I | satisfied | Delivery workers receive `NoticeDeliveryTarget`, not the full matching/index record. Admin projection and delivery have separate inputs. ACK-requested publication uses its explicit completion path; `should_wait_for_the_actor_when_a_publish_requests_an_ack` pins it. |
| D | satisfied | The family owner selects live targets; a separate worker uses the Router for handoff. No persistence dependency is introduced. `should_deliver_to_surviving_subscriber_when_sibling_subscriber_actor_stopped` covers a failing delivery collaborator. |

### Stream

| Principle | Status | Source/caller decision and evidence |
| --- | --- | --- |
| S | satisfied | `stream/actor.rs` owns only live append sessions; `store/` owns committed history and sequencing; `sink/session_owners.rs` owns session lookup; cleanup removes live state. `should_require_matching_stream_session_owner_identity` and the actor reference model cover ownership. |
| O | satisfied | Scope-specific compact fields implement `storage/compact_page_values.rs::PageRecordCodec`; the common envelope owns body/metadata/expiry. A scope codec change is separate from notification/session policy. `should_preserve_compact_page_codec_bytes` covers all four implementations. |
| L | satisfied | The four page codecs share encoded-length and decode/envelope contracts, not scope identity semantics. Golden bytes and `should_reject_corrupt_persisted_rows_for_each_stream_decoder_family` cover substitution. Session cleanup retains committed history; it does not promise replay of live notifications. |
| I | satisfied | Read parameter/result types, session owner lookups and notification targets expose their own roles. `sink/reads/` and `sink/delivery/` do not require a combined application workflow interface. Cursor/selector behavior is covered by `should_reject_resumed_read_with_cursor_fingerprint_from_a_different_selector`. |
| D | satisfied | `StreamActor` consumes a domain `StreamStore`; Midge and layout mechanics remain in store/storage. Injected `Clock` handles expiry. `should_not_retry_global_reservation_with_unknown_commit_outcome` pins the adapter's effect boundary. |

### KV

| Principle | Status | Source/caller decision and evidence |
| --- | --- | --- |
| S | satisfied | `kv/actor/transaction_access.rs` protects scope/read-only rules; transactions/snapshots/mutations are separate from inventory and wire handling. `ActiveKvTx` fields and actor transactions are private. `should_enforce_transaction_scope_to_single_resource` and `should_preserve_kv_writes_after_mismatched_commit_domain_invariant` exercise the owner. |
| O | satisfied | Another Midge error maps at `kv/store.rs::map_error`; transaction scope logic does not inspect backend wording. The base SCAN options change intentionally touched command/codec/scan behavior, not unrelated domain policy. `should_preserve_unknown_commit_outcome_as_a_backend_error` pins mapping semantics. |
| L | satisfied | `Actor::receive` delegates to the same typed handler; sink mailbox and session cleanup use shared runtime contracts. `should_route_kv_cleanup_through_family_actor` and `should_release_kv_resource_lock_after_session_cleanup` cover the concrete implementation. |
| I | satisfied | `kv/admin` exposes inspection request/result types while the domain endpoint remains crate-private. Transactions, watches and snapshots have distinct operations. Passive inventory callers do not acquire mutation authority from a projection. |
| D | satisfied | `kv/store.rs` is the sole data-transaction adapter. The actor retains the original transaction while admission waits. `should_preserve_original_snapshot_conflict_checks_after_admission_wait` covers the boundary without inventing an unused alternate-backend abstraction. |

### Queue

| Principle | Status | Source/caller decision and evidence |
| --- | --- | --- |
| S | satisfied | The actor owns ready/inflight/delayed invariants; `reservation_book`, storage, due-work and admin projection are separate. Private actor state prevents external partial mutation. `should_preserve_redelivery_after_stale_ack_domain_invariant` and the reservation state model cover transitions. |
| O | satisfied | Storage admission changes are classified by `QueueStoreError` and shared storage admission; actor mutation planning consumes that classification. Ready notification and wildcard reserve routing remain distinct from persistence classification. |
| L | satisfied | Queue's mailbox acceptance and cleanup do not promise durable acceptance or retained inflight ownership. `should_admit_session_cleanup_even_when_client_admission_is_exhausted` and `should_reject_stale_reserve_after_disconnect_cleanup_marks_session` cover the concrete cleanup implementation. |
| I | satisfied | `RoutedReservedMessage`, `QueueKey`, reservation book, admin snapshots and event deliveries are role-specific. Wildcard receive returns a concrete route without exposing storage internals. `should_reject_queue_inflight_followups_from_non_owner_session` pins the ownership boundary. |
| D | satisfied | `QueueActor` owns a domain `QueuePersistence` adapter, not a raw engine. Backend specifics remain in `recovery_store.rs` and storage modules; time is injected. `should_leave_unknown_ack_commit_outcome_terminal` and `should_wait_for_queue_storage_pressure_to_clear_before_committing` pin failure handling. |

### RPC

| Principle | Status | Source/caller decision and evidence |
| --- | --- | --- |
| S | satisfied | Registration, queued/active calls, worker credit, deadlines and response forwarding have distinct owners in `rpc/sink/state_model/` and `response_forwarder.rs`. `should_isolate_queued_rpc_expiration_with_shared_uuid_domain_invariant` and the cancellation reference model cover coordination. |
| O | satisfied | `RpcDispatchState`, `RpcRequestState` and `RpcResponseState` separate selection, registration and response changes. A selection change does not require forwarding to consume every `RpcState` method. `should_select_registration_through_dispatch_state_seam` exercises the role. |
| L | satisfied | Pending-response checks preserve worker ownership and advance sequence only after successful delivery. Cleanup/cancellation retain or release credit according to their contract. `should_reject_worker_response_from_non_owner_session_given_rpc_sink` and `should_hold_worker_credit_until_cleanup_ack_and_ignore_late_duplicate_release` exercise it. |
| I | satisfied | Worker registration consumes the registration role; response forwarding consumes `RpcResponseState`. The shared mailbox's optional frame-loop timing reports a capability, not a guaranteed scheduling lane for every sink. |
| D | satisfied | Dispatch policy operates on role traits and plain state; `RpcResponseForwarder` owns Router interaction. No durable store is pulled into RPC. `should_drop_late_worker_response_after_requester_cleanup_without_forward_error` covers detached delivery. |

### Lease

| Principle | Status | Source/caller decision and evidence |
| --- | --- | --- |
| S | satisfied | Family-owned lease/fencing state, waiter tracking, expiry, LIST snapshots and cleanup are explicit. Transfer `LeaseKey` has no hidden compiled cache. `should_reject_stale_lease_renewal_after_reacquire_domain_invariant` protects the owner. |
| O | satisfied | LIST validation and snapshot continuation live in the list path; they do not alter acquisition/fencing transitions. A selector or continuation rule is traced through list/validation and its codec contract. `should_scope_lease_list_to_requesting_family_only` covers that boundary. |
| L | satisfied | Mailbox/cleanup guarantees retain process-local fencing and release only the disconnected session. `should_confirm_lease_session_cleanup_before_reporting_delivery`, `should_reject_stale_acquire_after_disconnect_cleanup_marks_session` and `should_preserve_other_session_leases_given_session_cleanup` cover the implementation. |
| I | satisfied | LIST item/cursor data, acquire requests and subscription records are distinct from private ownership state. Holder incarnation is separate from logical owner ID and raw session identity. |
| D | satisfied | Coordination consumes runtime routing and indexed subscriptions; no durable store or cross-node ownership is assumed. Cleanup removes waiters before advancing released keys. Non-owner/stale-token fault tests protect those mechanics. |

### Schedule

| Principle | Status | Source/caller decision and evidence |
| --- | --- | --- |
| S | remediated | Private `schedule/cron.rs` owns validated expression/matcher state and next-occurrence calculation; `route_grammar.rs` owns identity parsing; actor/model owns the cached definition. Compile-fail mutation coverage and `schedule_cron_contract` prevent the reproduced divergence. |
| O | satisfied | Cron syntax changes are confined to its owner. A delivery-mode extension intentionally updates enum/format mappings, while pure candidate selection remains in `sink/delivery_strategy.rs`. Transport handoff does not implement the mode-selection match. |
| L | satisfied | `SchedulePersistence` claims/ACKs obey the supplied write policy; errors do not generally establish no effect. `should_surface_claim_error_from_fake_persistence`, persistence-failure actor tests and the claim reference model cover that seam. The Schedule `Actor` implementation is test-only. |
| I | remediated | The public request/list DTOs remain available, but actor-cache `ScheduleDef` is no longer exported. The intentional Rust source break is recorded as R-05. Existing cron/route/clock compatibility paths remain, without making internal callers depend on the protocol facade. |
| D | satisfied | Clock comes from the runtime owner; claim/ACK mechanics consume `SchedulePersistence`; store owns Midge conversion; recipient selection uses plain IDs. `should_not_advance_in_memory_or_fire_when_reschedule_persist_fails` covers the failure boundary. |

## Shared substitution and capability evidence

| Boundary | Implementations/callers reviewed | Common evidence and limits |
| --- | --- | --- |
| `MailboxSink` | Seven domain endpoint impls, runtime Mailbox, Router, actor references and session sinks | Existing all-domain normal cleanup plus `boot/domains/tests/contracts.rs::should_accept_control_lane_cleanup_for_every_registered_domain` cover both entry paths. `should_reject_both_delivery_lanes_for_every_stopped_domain_sink` calls both methods through every stopped `Arc<dyn MailboxSink>` and requires `ActorStopped`. Family pool and `tests/actor_delivery_contract.rs` cover priority and failure limits. Acceptance is not completion; post-handoff timeouts do not establish NOT_APPLIED. |
| `SessionScoped` | All seven `sink/cleanup.rs` implementations; shared default cleanup methods and envelope dispatch callers | `should_mark_session_before_releasing_resources` pins ordering; domain stale-request and nonempty cleanup tests pin each release implementation. Cleanup drops ephemeral resources; it does not remove committed data or restore session identity. |
| `RoutedSubscription` | Notice, Stream, KV watches, Queue, Lease and Schedule records and shared indexes | Record identity remains stable while registered. `should_reclaim_total_registration_capacity_after_removal` and `should_keep_wildcard_limit_separate_from_total_registration_limit`, plus per-domain foreign-owner/family tests, cover index behavior. RPC's registration table has a separate contract. |
| `Clock` | SystemClock and deterministic domain/test clocks | Monotonic elapsed time and epoch wall time are distinct references. Fixed-clock cron, expiry and wall-jump tests exercise consumers; time conversion does not assume wall-clock continuity. |
| `SchedulePersistence` | ScheduleStore and failure fake; claim/ACK policy helpers | Typed batches/policies are the interface. `should_surface_claim_error_from_fake_persistence` and `should_surface_ack_error_from_fake_persistence` exercise both operations through the substituted failure implementation. Store golden bytes and actor failure tests cover real storage/state. No blanket error-to-NOT_APPLIED inference is added. |
| RPC roles | RpcState implementations and registration, selection, response-forwarding callers | Role seam, wrong-worker, sequence/forwarding failure and cancellation-credit tests cover the relevant pre/postconditions. No unused generic repository layer is introduced. |
| `PageRecordCodec` | Global, realm, area and resource compact record implementations | `should_preserve_compact_page_codec_bytes` exercises all four against fixed bytes. Decode corruption tests cover malformed records. Length reporting and common-envelope interpretation are part of the contract. |
| `Actor` | Production KV, Stream area/realm actor implementations and runtime dispatch; Schedule test harness excluded from production applicability | Runtime actor tests and domain handler/session tests cover typed dispatch and lifecycle. No claim that every domain is interchangeable as the same actor message type. |

## Changes, validation and remaining release boundaries

The cron failure was reproduced natively before implementation: changing the
public weekday field left matchers stale and returned 86,400 seconds instead
of the equivalent newly parsed schedule's 345,600 seconds. A compile-fail
regression then failed before the fix because direct mutation still compiled;
it passes after field encapsulation. The existing cron evaluation algorithms
and persisted/wire codecs are unchanged.

The source compatibility changes are explicit in ledger rows R-04/R-05 and the
migration guide: use read-only cron accessors, parse a replacement expression,
and use public create/list DTOs instead of an actor cache model. Existing route,
clock and cron import paths remain covered by integration/benchmark consumers.

Final validation results and the reviewed commit are recorded in #499 and the
PR. Required gates are workspace tests (including golden/model tests), public
cron contract and compile-fail tests, formatting, strict all-target/all-feature
Clippy, and shared test/module/benchmark metadata validation. A full hosted
broker check is a separate current-commit result, not implied by this document.

Midge upgrades and final storage/migration qualification remain separate release
work. SDK canonical-coverage reconciliation and hosted Wire 2 qualification are
also separate. This review does not authorize merging or publication; #498
stays draft and unmerged under the maintainer's Midge hold.
