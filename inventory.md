# Test & Bench Inventory

_Generated 2026-01-01T20:02:16Z by `scripts/generate_inventory.py`._

Complete inventory of all test and benchmark functions across midge.

**Src Tests**

- `src/api/ingress.rs`
  - tests:
    - `should_configure_with_builder_methods`
    - `should_create_default_ingress_config`
    - `should_display_ingress_errors`

- `src/api/tcp.rs`
  - tests:
    - `should_encode_length_prefix`
    - `should_generate_unique_session_ids`
    - `should_handle_large_frames`

- `src/api/ws.rs`
  - tests:
    - `should_generate_unique_session_ids`

- `src/domains/lease/actor.rs`
  - tests:
    - `should_acquire_unowned_lease`
    - `should_allow_expired_lease_takeover`
    - `should_allow_reacquire_after_release`
    - `should_issue_monotonic_fencing_tokens`
    - `should_query_lease_status`
    - `should_reject_acquire_when_held_by_other`
    - `should_reject_release_with_wrong_token`
    - `should_reject_renew_of_expired_lease`
    - `should_reject_renew_with_wrong_token`
    - `should_release_lease_with_valid_token`
    - `should_renew_lease_with_valid_token`
    - `should_return_existing_token_for_idempotent_acquire`

- `src/domains/lease/guard.rs`
  - tests:
    - `should_create_lease_handle_from_acquired_response`
    - `should_create_lease_handle_from_already_held_response`
    - `should_independently_manage_leases_across_families`
    - `should_isolate_leases_across_route_families`
    - `should_lose_all_leases_on_simulated_restart`
    - `should_mark_handle_invalid_after_expiration`
    - `should_prevent_conflicts_within_same_route_family`
    - `should_proactively_expire_leases_on_tick`
    - `should_reject_stale_fencing_tokens`
    - `should_return_error_when_fenced`
    - `should_return_error_when_lease_held_by_other`
    - `should_return_error_when_lease_not_held`
    - `should_serialize_concurrent_acquires_correctly`

- `src/domains/notification/actor.rs`
  - tests:
    - `should_allow_multiple_sessions_on_same_actor`
    - `should_clean_up_on_session_disconnect`
    - `should_create_notice_route_actor`
    - `should_support_idempotent_unsubscribe`
    - `should_track_subscriptions`
    - `should_trust_session_actor_for_auth`

- `src/protocol/mux.rs`
  - tests:
    - `should_map_default_ranges`
    - `should_route_to_channel`
    - `should_track_backpressure`

- `src/runtime/actor.rs`
  - tests:
    - `should_compare_equal_actor_ids`
    - `should_compare_unequal_actor_ids`
    - `should_create_actor_id`
    - `should_create_context_with_running_state`
    - `should_fail_send_when_mailbox_full`
    - `should_format_actor_id`
    - `should_get_actor_id_from_ref`
    - `should_inherit_causation_when_sending_from_context`
    - `should_inherit_deadline_when_sending_from_context`
    - `should_reply_to_sender_via_context`
    - `should_send_message_via_actor_ref`
    - `should_send_message_via_context`
    - `should_stop_context`

- `src/runtime/context.rs`
  - tests:
    - `should_cancel_timer`
    - `should_fire_timer_after_delay`
    - `should_schedule_repeating_timer`

- `src/runtime/envelope.rs`
  - tests:
    - `should_create_envelope_with_destination`
    - `should_create_envelope_with_source`
    - `should_create_reply_envelope`
    - `should_detect_expired_deadline`
    - `should_extract_payload`
    - `should_format_message_id`
    - `should_generate_unique_message_ids`
    - `should_inherit_deadline_in_reply`
    - `should_return_none_for_wrong_type`
    - `should_set_causation`
    - `should_set_deadline`

- `src/runtime/mailbox.rs`
  - tests:
    - `should_clone_mailbox`
    - `should_create_mailbox_with_capacity`
    - `should_handle_multiple_senders`
    - `should_receive_envelope_from_mailbox`
    - `should_respect_mailbox_capacity`
    - `should_send_envelope_to_mailbox`

- `src/runtime/matcher.rs`
  - tests:
    - `should_match_double_star_at_end`
    - `should_match_double_star_at_end_many_segments`
    - `should_match_double_star_at_end_no_segments`
    - `should_match_double_star_from_middle`
    - `should_match_double_star_from_middle_many_segments`
    - `should_match_double_star_from_middle_no_segments`
    - `should_match_double_star_with_no_segments`
    - `should_match_double_star_with_related_prefix`
    - `should_match_exact_route`
    - `should_match_multiple_wildcards`
    - `should_match_multiple_wildcards_inventory`
    - `should_match_pattern_without_scheme`
    - `should_match_pattern_without_scheme_update`
    - `should_match_single_star_wildcard`
    - `should_match_single_star_wildcard_delete`
    - `should_match_single_star_wildcard_update`
    - `should_not_match_across_single_star_boundary`
    - `should_not_match_different_route`
    - `should_not_match_double_star_across_unrelated_prefix`
    - `should_not_match_literal_when_pattern_expects_wildcard`
    - `should_not_match_multiple_wildcards_insufficient_segments`

- `src/runtime/router.rs`
  - tests:
    - `should_clone_router`
    - `should_handle_concurrent_routing`
    - `should_isolate_same_route_in_different_families`
    - `should_register_route`
    - `should_return_error_for_failed_delivery`
    - `should_return_error_for_unregistered_route`
    - `should_route_envelope_to_registered_route`
    - `should_support_multiple_routes`
    - `should_unregister_route`

- `src/runtime/routing.rs`
  - tests:
    - `should_allow_same_route_in_different_families_in_hashmap`
    - `should_compare_route_families_by_identity`
    - `should_compare_routes_by_path`
    - `should_create_route`
    - `should_create_route_address`
    - `should_create_route_family`
    - `should_hash_route_families_consistently`
    - `should_isolate_same_route_in_different_families`

- `src/runtime/scheduler.rs`
  - tests:
    - `should_create_scheduler_with_workers`
    - `should_drop_expired_messages`
    - `should_enable_actor_to_actor_messaging`
    - `should_generate_unique_actor_ids`
    - `should_process_messages_in_sequence`
    - `should_start_scheduler`
    - `should_stop_scheduler`
    - `should_support_reply_pattern`

- `src/runtime/subscriptions.rs`
  - tests:
    - `should_handle_mixed_patterns`
    - `should_handle_multiple_subscribers_to_same_pattern`
    - `should_isolate_by_route_family`
    - `should_isolate_families_independently`
    - `should_match_double_star_multiple_segments`
    - `should_match_double_star_with_suffix`
    - `should_match_double_star_zero_segments`
    - `should_match_exact_pattern`
    - `should_match_single_star_wildcard`
    - `should_not_cross_realm_with_double_star`
    - `should_not_cross_star_boundary`
    - `should_not_match_different_route`
    - `should_remove_subscription`

- `src/runtime/supervision.rs`
  - tests:
    - `should_create_escalate_strategy`
    - `should_create_restart_strategy`
    - `should_create_resume_strategy`
    - `should_create_stop_strategy`
    - `should_decide_restart_action`
    - `should_decide_stop_action`
    - `should_expire_old_restart_records`
    - `should_fail_restart_when_exceeding_limit`
    - `should_reset_restart_tracker`
    - `should_track_restart_within_limit`

- `src/session/ingress.rs`
  - tests:
    - `should_list_sessions`
    - `should_retrieve_session_info`

**Integration Tests (tests/)**

(none)

**Benches & Stress (benches/)**

- `benches/tier1_hotpath_matcher.rs`
  - benches & stress:
    - `bench_backtracking_knee`
    - `bench_depth_knee`
    - `bench_double_star_end`
    - `bench_double_star_middle`
    - `bench_exact_match`
    - `bench_negative_match_late_fail`
    - `bench_pattern_complexity_knee`
    - `bench_single_wildcard`

- `benches/tier1_hotpath_reply.rs`
  - benches & stress:
    - `bench_reply`

- `benches/tier1_hotpath_self_send.rs`
  - benches & stress:
    - `bench_self_send`

- `benches/tier1_hotpath_send_local.rs`
  - benches & stress:
    - `bench_send_local`

- `benches/tier2_subsystem_lease.rs`
  - benches & stress:
    - `bench_lease_creation`
    - `bench_lease_family_isolation`
    - `bench_lease_runtime_acquire_release_loop`
    - `bench_lease_runtime_burst_load`
    - `bench_lease_runtime_contended_acquire`
    - `bench_lease_runtime_multi_family_isolation`
    - `bench_lease_runtime_renew_spin`
    - `bench_lease_runtime_sustained_load`
    - `bench_lease_spawn`

- `benches/tier2_subsystem_mailbox.rs`
  - benches & stress:
    - `bench_mailbox_capacity`
    - `bench_mailbox_send`

- `benches/tier2_subsystem_router.rs`
  - benches & stress:
    - `bench_route_creation`
    - `bench_route_family_isolation`

- `benches/tier2_subsystem_scheduler.rs`
  - benches & stress:
    - `bench_scheduler_spawn`
    - `bench_scheduler_spawn_cross_family`

- `benches/tier2_subsystem_subscriptions.rs`
  - benches & stress:
    - `bench_insert_single_pattern`
    - `bench_insert_with_double_star`
    - `bench_insert_with_single_star`
    - `bench_match_depth_10`
    - `bench_match_depth_3`
    - `bench_match_depth_5`
    - `bench_match_double_star`
    - `bench_match_exact_pattern`
    - `bench_match_fanout_dense_100`
    - `bench_match_fanout_sparse_100`
    - `bench_match_single_star`
    - `bench_mixed_insert_remove_match`
    - `bench_remove_subscription`
