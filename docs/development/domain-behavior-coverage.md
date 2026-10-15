# Domain behavior and invariant coverage

Reviewed against [domain boundaries](domain-boundaries-spec.md), the
[architectural laws](architectural-laws.md), and the
[routing contract](routing-design.md) on 2026-10-15.

This map records executable evidence for the supported contract. It is not a
line-coverage percentage or a proof of every interleaving. Actor reconstruction,
engine reopening, broker restart, and abrupt process exit are different evidence
boundaries. In-memory reconstruction does not establish crash durability.

## Shared boundaries

| Invariant | Executable evidence |
| --- | --- |
| Authorization must cover a selector's entire match set; partial grants cannot be unioned | `src/session/permissions.rs`: `should_reject_registration_exceeding_permission_match_set`, `should_not_combine_partial_grants_to_authorize_registration`; `src/api/runtime_ingress/tests/authorization_routes.rs`: `should_apply_depth_aware_registration_authorization_to_every_fixed_depth_domain` |
| Codec, route classification, and authorization agree | `src/api/runtime_ingress/tests/authorization_routes_kv.rs`: `should_accept_kv_begin_authorization_exactly_when_the_kv_codec_accepts_it`; domain protocol and transport suites |
| Realm and RouteFamily isolation remain separate from domain composition | `tests/multi_domain_e2e.rs`: `should_keep_realms_isolated_while_multiple_domains_are_active_tcp` and `_ws`; domain-specific family tests below |
| Domains do not borrow one another's guarantees | `tests/multi_domain_e2e.rs`: Queue/Lease independence, Notice/Stream independence, RPC/Stream coexistence, and KV/Lease composition over TCP and WebSocket |
| Duplicate registration is idempotent before quotas; overlaps remain independent | Notice `delivery_and_limits.rs`, Schedule `delivery_and_subscriptions.rs`, RPC `wildcard_registrations.rs`, Lease subscription transport tests, and shared subscription-state tests |
| Disconnect removes session state; rejected response delivery cannot leave an inaccessible registration or ownership grant | Each domain's sink cleanup/fault tests and `tests/*_e2e.rs` transport suites |

Paths in the tables below are relative to `src/domains/<domain>/` unless a
`tests/` prefix is shown. Test names identify the behavior rather than serving
as assertions about implementation spelling.

## Notice

| Invariant | Executable evidence |
| --- | --- |
| Live fanout, wildcard matching, overlapping registrations, duplicate registration, bounded quotas | `sink/tests/delivery_and_limits.rs`; `tests/notice_e2e.rs` |
| Backpressure or failed delivery must not block another family or erase surviving subscribers | `sink/tests/publish.rs`: `should_keep_notice_family_responsive_while_delivery_worker_is_blocked`; `sink/tests/fault_injection.rs`: `should_deliver_to_surviving_subscriber_when_sibling_subscriber_actor_stopped` |
| Cleanup and restart do not restore live subscriptions | `sink/tests/cleanup.rs`; `tests/notice_e2e.rs`: `should_require_resubscribe_after_broker_restart_tcp` and `_ws` |
| A foreign session cannot unsubscribe a live subscriber | `sink/tests/invariants.rs`: `should_preserve_notice_subscription_after_foreign_unsubscribe_domain_invariant` verifies actual subsequent delivery and its subscription identity |

Notice tests assert live attempts and cleanup. They do not establish replay,
durable delivery, or ordering between subscribers.

## Stream

| Invariant | Executable evidence |
| --- | --- |
| Staged data stays invisible; commit/abort/disconnect transitions agree with an independent state oracle | `actor/tests/state_model.rs`: `should_match_stream_reference_state_model` |
| Storage state agrees with a generated oracle across reopening, expiration, and compaction | `store/tests/model_based.rs`: `should_match_reference_model_through_reopen_expiration_plus_compaction` |
| Ordered offsets and committed resource/area/realm visibility survive failures and restart | `store/tests/offsets_and_reads.rs`, `store/tests/sessions_layout_and_watermarks.rs`, `sink/tests/watermark_wiring.rs`; `tests/stream_e2e/restart_storage.rs` |
| Sync in background-cloud mode waits for cloud WAL acknowledgement | `store/tests/commit_persistence.rs`: `should_acknowledge_cloud_wal_before_sync_stream_commit_returns_in_background_mode` |
| Examined replay data is integrity-checked, including filtered records; limits bound blob hydration | `store/tests/bounded_replay.rs` covers missing and checksum-invalid blobs excluded by filters at resource, area, realm and global scope; `store/tests/compacted_locator_cache.rs` |
| Live subscriptions do not become replay cursors or recover after disconnect | `tests/stream_e2e/subscriptions_disconnect.rs` |
| Foreign owner/session finalization cannot consume staged work or publish it | `actor/tests/invariants.rs`: `should_preserve_stream_session_after_foreign_finalization_domain_invariant`, both Buffered and Sync, followed by the real owner's commit and exact readback |

## KV

| Invariant | Executable evidence |
| --- | --- |
| Commit, rollback, stale handles, scope rejection, and actor recreation agree with an independent state oracle | `actor/tests/state_model.rs`: `should_match_kv_reference_state_model` |
| Resource, realm, and family isolation; conflicts; read-only mutation rejection | `actor/tests/lifecycle.rs`, `actor/tests/scope.rs`, `actor/tests/conflict_and_error_paths.rs` |
| Persistence is chosen at COMMIT: Buffered avoids sync, Sync waits for local/cloud acknowledgement | `actor/tests/commit_persistence.rs`; wire codec/ingress persistence tests |
| Only committed current values recover after reopening; disconnected handles remain invalid | `tests/kv_advanced.rs`: `should_restore_committed_kv_value_on_engine_restart`, `should_discard_uncommitted_kv_write_on_engine_restart`; `tests/kv_e2e/tcp.rs`: `should_reject_kv_operation_after_disconnect_given_old_transaction_id` |
| Rejected commit preserves the original transaction's staged value for a valid commit | `actor/tests/scope.rs`: `should_preserve_kv_writes_after_mismatched_commit_domain_invariant`, varying family, realm, area, and resource independently |

## Queue

| Invariant | Executable evidence |
| --- | --- |
| Generated reservation ownership and visibility across ACK, extension, expiry and disconnect | `actor/tests/state_model.rs`: `should_match_queue_reservation_reference_state_model`, 256 bounded sequences with checks after every transition |
| Exclusive reservation, token checks, expiration, extension, delayed visibility, redelivery, and dead-letter transition | `actor/tests/inflight_and_delivery.rs`, `actor/tests/recovery_and_storage.rs`; `tests/queue_e2e.rs` |
| Failed ACK/redelivery persistence retains recoverable live state; ambiguous outcomes are not silently retried | `actor/tests/recovery_and_storage.rs`; `actor/tests/ack_admission.rs` |
| Incomplete split records are reconciled; malformed authoritative data fails closed; families stay isolated | `actor/tests/startup_reconciliation.rs`; `actor/tests/storage_index_and_core.rs` |
| Best-effort Queue writes skip WAL; explicitly flushed state can recover after process exit | `actor/tests/fast_flush.rs`: `should_keep_local_queue_writes_out_of_wal`, `should_recover_acknowledged_fast_queue_state_after_process_exit` |
| A stale ACK cannot erase a new reservation of the same message by the same session | `actor/tests/invariants.rs`: `should_preserve_redelivery_after_stale_ack_domain_invariant`, using a deterministic clock and validating the current token still ACKs successfully |

`tests/queue_advanced.rs` reconstructs actors over retained in-memory engines;
its names explicitly describe that boundary. Those tests are not crash-loss
qualification. Queue acceptance and ACKs do not guarantee crash recovery or
exactly-once delivery. The process-exit test above explicitly flushes its fixture;
it does not promise recovery of unflushed writes.

## RPC

| Invariant | Executable evidence |
| --- | --- |
| Correlation ownership, ordered streaming chunks, terminal cleanup, and undeliverable response handling | `sink/tests/response_sequence.rs`, `sink/tests/cleanup_and_worker_errors.rs` |
| Bounded credit, route-local FIFO, wildcard credit sharing, fair rotation, and family isolation | `sink/tests/wildcard_registrations.rs`, `sink/tests/timeouts_and_capacity.rs` |
| Generated cleanup order preserves credit until the assigned worker acknowledges; repeated/stale/wrong-family/wrong-worker ACKs and late terminal responses cannot release another invocation | `sink/tests/cancellation/safety/state_model.rs`: `should_preserve_rpc_credit_across_cleanup_order_state_model`, 256 bounded sequences |
| Cancellation retains dispatched credit until cleanup acknowledgement; stale cleanup cannot affect reused correlation | `sink/tests/cancellation/safety.rs`; `sink/tests/cancellation/frame_loop_grace.rs`; `tests/rpc_e2e/cancellation.rs` |
| Restart drops pending requests and requires worker registration again | `tests/rpc_e2e/restart.rs` |
| Queued expiration for one family cannot remove an identical UUID in another family | `sink/state_model/pending_table/tests.rs`: `should_isolate_queued_rpc_expiration_with_shared_uuid_domain_invariant` |

RPC's production `sink/state_model/` directory is distinct from generated tests
under `sink/tests/cancellation/safety/state_model.rs`. The generated test drives
the real dispatcher and sinks, with an independent credit-ownership oracle.
Cancellation does not prove side-effect
rollback or retry safety.

## Lease

| Invariant | Executable evidence |
| --- | --- |
| One holder, session ownership, stale-token rejection, release, and expiry | `sink/tests/fault_injection.rs`, `sink/tests/correctness.rs`; `tests/lease_e2e/core.rs` |
| A disconnected queued waiter cannot receive ownership when the holder expires; the surviving waiter advances | `sink/tests/waiter_lifecycle.rs`: `should_skip_disconnected_waiter_when_holder_expires`, using explicit expiry and confirmed session cleanup |
| Waiter ordering and failed grant delivery do not leak ownership | `sink/tests.rs`: `should_promote_next_waiter_when_prior_waiter_cannot_receive_grant`; `tests/lease_e2e/waiters.rs` |
| Observation is family/session isolated; cursor misuse, TTL, snapshot memory and wire budgets are bounded | `sink/tests/list.rs`; `tests/lease_e2e/list.rs` and `subscriptions.rs` |
| Disconnect and restart destroy ownership | `tests/lease_e2e/core.rs`: `should_remove_session_owned_leases_when_client_disconnects_*`, `should_lose_all_leases_on_broker_restart_*` |
| A previous acquisition's token cannot renew a reacquired lease, while its current token remains usable | `sink/tests/fault_injection.rs`: `should_reject_stale_lease_renewal_after_reacquire_domain_invariant`, covering raw and prepared ingress paths |

Fencing monotonicity is process-local. Renewal need not allocate a new token;
reacquisition must fence the prior acquisition. Cross-restart fencing continuity
is not supported.

## Schedule

| Invariant | Executable evidence |
| --- | --- |
| Generated definition/claim lifecycle agrees with an independent oracle through create/upsert, cancel, scan, ACK, forward time and actor reconstruction | `actor/tests/state_model.rs`: `should_match_schedule_claim_reference_state_model`, 128 bounded sequences; live definitions and live/persisted claims checked after every transition |
| Due-time safety under forward/backward clock changes; overdue schedules skip forward | `tests/schedule_advanced.rs`; `actor/tests.rs`: `should_normalize_overdue_persisted_schedule_forward_on_preload` |
| Durable definitions preload before traffic; pending claims persist without age-based expiry | `tests/schedule_e2e.rs`: `should_recover_schedule_definitions_before_accepting_schedule_traffic`; `store/tests.rs`; `sink/tests/recovery_and_claims.rs` |
| Failed create/upsert/cancel/claim/ACK persistence does not advance live state or duplicate same-broker publication | `actor/tests/more.rs`; `sink/tests/recovery_and_claims.rs`: `should_retry_ack_without_republishing_given_same_broker_ack_persist_failure` |
| Cancel/reschedule ordering, delivery modes, overlapping subscriptions, family isolation and cleanup | `actor/tests.rs`, `actor/tests/more.rs`; `sink/tests/delivery_and_subscriptions.rs`, `fault_injection.rs` |
| An invalid batch must not partially upsert an existing definition or insert new definitions | `actor/tests/invariants.rs`: `should_preserve_schedule_definition_after_invalid_batch_domain_invariant`, checking live state and persisted definitions including the original due time |

Schedule claims are timing intent, not a durable subscriber-delivery guarantee
or workflow execution history.

## Running and interpreting the evidence

```sh
cargo test --locked --lib domain_invariant
PROPTEST_CASES=256 cargo test --locked --lib state_model
cargo test --locked --workspace
cntryl-tools validate-tests
cntryl-tools check-module-sizes
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic
```

The generated models cover bounded KV, Stream, Queue and Schedule operation
sequences, plus RPC cancellation/cleanup ordering. Stream also has a generated
storage model for reopening, expiration and compaction. Notice and Lease use
deterministic scenario and fault tests. These models do not prove every
concurrency interleaving; Schedule reconstruction retains the same engine and
is not process-crash qualification.

External resource-capped and S3 qualification remains separate:
`tests/constrained_runtime.rs` documents its prerequisites and ignored cases;
`tests/constrained_runtime/s3/fixture.rs` additionally requires
`recovery-qualification`. Run those campaigns using
[constrained-runtime.md](constrained-runtime.md), rather than counting ignored
cases as passes. Simulated-cloud unit tests do not qualify real provider outages
or large cold-cache WAL recovery.

This audit retains the existing Midge dependency revision. It does not qualify
or replace the pending official Midge 0.4.0 release. Rerun the correctness gates
and the relevant real-provider recovery qualification after adopting that
release; do not infer that result from tests against the current revision.

## Queue attempt-count contract follow-up

During model development, `Send -> Reserve(session 2) -> Disconnect(session 2)
-> Reserve(session 1)` returned attempt 1 on both deliveries. Expiry-driven
redelivery advances the reported count. Immediate session cleanup returns work
to ready without advancing the stored retry counter. The internal record comment
currently describes increments on successful inflight assignment, while the
canonical domain contract does not specify the disconnect case.

The new Queue oracle checks ownership, visibility, fresh tokens, token/session
rejection, retention and successful ACK, without inventing attempt-count or
retry-budget semantics for disconnect. Define the intended attempt-count and
dead-letter-budget behavior for disconnect and failed handoff explicitly before
changing that policy. This observation does not establish crash-safe attempt
counting or a production durability guarantee.
