# Performance Target Rubric v2.3

Fitz tracks performance targets in two forms:

- Human-readable rubric matrix: this document
- Machine-readable mirror: [config/perf_targets.json](../../config/perf_targets.json)

This version is generated from current `cntryl-stress.v2` artifacts. Current values are normalized to `mean_us` from each selected row: throughput rows are converted as `1_000_000 / ops_per_second`, and `ns_per_op` rows are divided by 1,000. `mean_us` remains the canonical target metric.

## Baseline

All targets assume the same local single-node baseline:

- CPU: 16 vCPU
- Storage: NVMe SSD
- Network: Loopback for tier4, raw numbers without TLS
- RAM: 64-128 GB
- Build: release
- Primary gate: mean_us

Derived `min ops/sec` is `1_000_000 / max_mean_us`. It is shown for quick scanning, but `mean_us` remains the canonical gate.

## Class Model

- `engine_core`: tier3 system-domain paths. This is the headline product truth.
- `service_budget`: tier4 product paths, split into `direct_api`, `transport`, and `contention`.
- `internal_explainer`: tier2 subsystem rows that explain product movement but do not outrank product-facing misses.
- `hard`: included in scoreboards and hotspot selection.
- `variance_gated`: included only when current `rel_stddev <= max_rel_stddev`.
- `informational`: visible only, excluded from attainment rollups and hotspot selection.

## Perf-Loop Selection

The perf loop uses [config/perf_targets.json](../../config/perf_targets.json) as its source of truth and selects hotspots in this order:

1. `engine_core`
2. `service_budget/direct_api`
3. `service_budget/transport`
4. `service_budget/contention`
5. `internal_explainer`

Within a bucket, the selector ranks by percent over `operational_target`, then percent over `stretch_target`, then current `mean_us`.

## Engine Core

All rows in this section map to `target_class = engine_core`.

| domain | suite | scenario | current us | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| kv | tier3-system-kv | dual_family_concurrent | 0.11563 | 1.8 | 555,556 | 1.2 | 833,333 | hard |  |
| kv | tier3-system-kv | mixed_read_write_families | 0.09141 | 1.3 | 769,231 | 0.9 | 1,111,111 | hard |  |
| kv | tier3-system-kv | single_family_intensive | 0.233772 | 15 | 66,667 | 10 | 100,000 | hard |  |
| kv | tier3-system-kv | triple_family_contention | 0.09963 | 1.3 | 769,231 | 0.9 | 1,111,111 | hard |  |
| lease | tier3-system-lease | dual_route_concurrent | 5.954408 | 1.8 | 555,556 | 1.3 | 769,231 | hard | 2026-07-06 matrix did not confirm 6.506us as stable; current/pre-audit matched within 10%, but row was noisy and remains unresolved against the 4.616us baseline. |
| lease | tier3-system-lease | mixed_operations_high_load | 1.787775 | 1.3 | 769,231 | 1 | 1,000,000 | hard |  |
| lease | tier3-system-lease | single_route_intensive | 4.504248 | 2.2 | 454,545 | 1.6 | 625,000 | hard |  |
| lease | tier3-system-lease | triple_route_contention | 0.503941 | 1.05 | 952,381 | 0.8 | 1,250,000 | hard |  |
| notice | tier3-system-notice | high_subscriber_count | 7.833007 | 24 | 41,667 | 16 | 62,500 | hard |  |
| notice | tier3-system-notice | pattern_matching | 0.563055 | 0.55 | 1,818,182 | 0.4 | 2,500,000 | hard |  |
| notice | tier3-system-notice | sustained_fanout | 0.580767 | 1.5 | 666,667 | 1 | 1,000,000 | hard |  |
| queue | tier3-system-queue | bulk_recovery | 0.261657 | 0.65 | 1,538,462 | 0.45 | 2,222,222 | hard |  |
| queue | tier3-system-queue | high_contention | 0.925769 | 1.5 | 666,667 | 1 | 1,000,000 | hard |  |
| queue | tier3-system-queue | mixed_steady_state | 1.977406 | 3.4 | 294,118 | 2.5 | 400,000 | hard |  |
| queue | tier3-system-queue | sustained_load | 0.928568 | 1.5 | 666,667 | 1 | 1,000,000 | hard |  |
| rpc | tier3-system-rpc | scaling_256_full_roundtrip | 0.731677 | 7 | 142,857 | 5 | 200,000 | hard |  |
| rpc | tier3-system-rpc | scaling_64_full_roundtrip | 0.732367 | 2.8 | 357,143 | 2 | 500,000 | hard |  |
| rpc | tier3-system-rpc | short_roundtrip_batch | 0.736302 | 3 | 333,333 | 2 | 500,000 | hard |  |
| rpc | tier3-system-rpc | single_response_throughput | 0.725264 | 1.7 | 588,235 | 1.2 | 833,333 | hard |  |
| rpc | tier3-system-rpc | steady_state_tracking | 0.728219 | 1.6 | 625,000 | 1.1 | 909,091 | hard |  |
| rpc | tier3-system-rpc | sustained_dispatch | 0.725878 | 2 | 500,000 | 1.3 | 769,231 | hard |  |
| schedule | tier3-system-schedule | collect_due_occurrences_not_ready_1000 | 0.03783 | 0.075 | 13,333,333 | 0.055 | 18,181,818 | hard |  |
| schedule | tier3-system-schedule | collect_due_occurrences_partial_ready_1000 | 5.377574 | 1.4 | 714,286 | 1 | 1,000,000 | hard |  |
| schedule | tier3-system-schedule | list_uncached_9_of_10 | 0.00525 | 0.015 | 66,666,667 | 0.01 | 100,000,000 | hard |  |
| schedule | tier3-system-schedule | list_uncached_99_of_100 | 0.001867 | 0.008 | 125,000,000 | 0.006 | 166,666,667 | hard |  |
| schedule | tier3-system-schedule | list_uncached_999_of_1000 | 0.001657 | 0.0075 | 133,333,333 | 0.0055 | 181,818,182 | hard |  |
| stream | tier3-system-stream | batch_write | 0.239311 | 1.538 | 650,195 | 1.25 | 800,000 | hard |  |
| stream | tier3-system-stream | multiarea_writes | 0.243811 | 1.538 | 650,195 | 1.25 | 800,000 | hard |  |
| stream | tier3-system-stream | offset_tracking | 4.806314 | 6.667 | 149,993 | 5 | 200,000 | hard |  |
| stream | tier3-system-stream | publish_fanout | 48.079504 | 1,818.182 | 550 | 1,538.462 | 650 | hard |  |
| stream | tier3-system-stream | read_area_wildcard | 0.129668 | 1.538 | 650,195 | 1.25 | 800,000 | hard |  |
| stream | tier3-system-stream | read_realm_wildcard | 0.16021 | 1.6 | 625,000 | 1.333 | 750,188 | hard |  |
| stream | tier3-system-stream | read_scan | 0.037743 | 1.176 | 850,340 | 1 | 1,000,000 | hard |  |
| stream | tier3-system-stream | sustained_append | 0.262652 | 2 | 500,000 | 1.538 | 650,195 | hard |  |

## Service Budget: Direct API

All rows in this section map to `target_class = service_budget` and `budget_group = direct_api`.

| domain | suite | scenario | layer | current us | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| kv | tier4-kv-gate | transaction_sequence | direct | 0.458435 | 25 | 40,000 | 15 | 66,667 | hard |  |
| kv | tier4-kv-gate | transaction_sequence | encoded | 0.597268 | 14 | 71,429 | 10 | 100,000 | hard |  |
| lease | tier4-lease-gate | acquire_release | direct | 0.431263 | 2.5 | 400,000 | 1.8 | 555,556 | hard |  |
| notice | tier4-notice-publish | publish | direct | 0.601693 | 6.5 | 153,846 | 4 | 250,000 | hard | mode=delivery_confirmed |
| queue | tier4-queue-gate | enqueue | direct | 18.642748 | 350 | 2,857 | 225 | 4,444 | hard |  |
| queue | tier4-queue-gate | enqueue | encoded | 18.740233 | 500 | 2,000 | 325 | 3,077 | hard |  |
| rpc | tier4-rpc-roundtrip | request_response | direct | 0.681696 | 28 | 35,714 | 18 | 55,556 | hard | mode=sync_single_inflight |
| rpc | tier4-rpc-roundtrip | request_response | encoded | 0.713301 | 9 | 111,111 | 6 | 166,667 | hard | mode=sync_single_inflight |
| schedule | tier4-schedule-lifecycle | create | direct | 64.401458 | 1,700 | 588 | 1,300 | 769 | hard |  |

## Service Budget: Transport

All rows in this section map to `target_class = service_budget` and `budget_group = transport`.

| domain | suite | scenario | layer | current us | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| kv | tier4-kv-gate | transaction_sequence | tcp | 33.809857 | 70 | 14,286 | 50 | 20,000 | hard |  |
| kv | tier4-kv-gate | transaction_sequence | websocket | 34.929051 | 110 | 9,091 | 80 | 12,500 | hard |  |
| lease | tier4-lease-gate | acquire_release | tcp | 29.246439 | 55 | 18,182 | 40 | 25,000 | hard |  |
| lease | tier4-lease-gate | acquire_release | websocket | 31.255469 | 55 | 18,182 | 40 | 25,000 | hard |  |
| notice | tier4-notice-publish | publish | tcp | 6.557107 | 75 | 13,333 | 50 | 20,000 | hard | mode=delivery_confirmed |
| notice | tier4-notice-publish | publish | websocket | 4.270308 | 70 | 14,286 | 50 | 20,000 | hard | mode=delivery_confirmed |
| notice | tier4-notice-publish | publish_unacked | tcp | 5.925513 | 75 | 13,333 | 50 | 20,000 | hard | mode=fire_and_forget_unacked |
| notice | tier4-notice-publish | publish_unacked | websocket | 2.056227 | 70 | 14,286 | 50 | 20,000 | hard | mode=fire_and_forget_unacked |
| queue | tier4-queue-gate | enqueue | tcp | 54.739589 | 210 | 4,762 | 150 | 6,667 | hard |  |
| queue | tier4-queue-gate | enqueue | websocket | 52.203944 | 375 | 2,667 | 250 | 4,000 | hard |  |
| rpc | tier4-rpc-roundtrip | request_response | tcp | 44.900579 | 60 | 16,667 | 40 | 25,000 | hard | mode=sync_single_inflight |
| rpc | tier4-rpc-roundtrip | request_response | websocket | 46.741819 | 160 | 6,250 | 100 | 10,000 | hard | mode=sync_single_inflight |
| rpc | tier4-rpc-pipeline | request_response_pipelined | tcp | 8.505402 | 60 | 16,667 | 40 | 25,000 | hard | mode=async_pipelined |
| rpc | tier4-rpc-pipeline | request_response_pipelined | websocket | 5.189162 | 160 | 6,250 | 100 | 10,000 | hard | mode=async_pipelined |
| schedule | tier4-schedule-lifecycle | batch_create | websocket | 4.89118 | 60 | 16,667 | 45 | 22,222 | hard |  |
| schedule | tier4-schedule-lifecycle | create | tcp | 52.885673 | 1,700 | 588 | 1,300 | 769 | hard |  |
| schedule | tier4-schedule-lifecycle | create | websocket | 51.255267 | 1,700 | 588 | 1,300 | 769 | hard |  |
| stream | tier4-stream-gate | append_open_session | tcp | 363.179956 | 1,000 | 1,000 | 750 | 1,333 | hard | record_metric=ns_per_op |
| stream | tier4-stream-gate | append_open_session | websocket | 420.279425 | 1,000 | 1,000 | 750 | 1,333 | hard | record_metric=ns_per_op |
| stream | tier4-stream-gate | rollback_lifecycle | tcp | 2,933.281225 | 5,000 | 200 | 3,750 | 267 | hard | record_metric=ns_per_op |
| stream | tier4-stream-gate | rollback_lifecycle | websocket | 2,979.490675 | 5,000 | 200 | 3,750 | 267 | hard | record_metric=ns_per_op |
| stream | tier4-stream-gate | read_resource_exact | tcp | 489.871356 | 1,000 | 1,000 | 750 | 1,333 | hard | read_scope=resource; record_metric=ns_per_op |
| stream | tier4-stream-gate | read_resource_exact | websocket | 521.404694 | 1,000 | 1,000 | 750 | 1,333 | hard | read_scope=resource; record_metric=ns_per_op |
| stream | tier4-stream-gate | read_area_wildcard | tcp | 483.374737 | 1,000 | 1,000 | 750 | 1,333 | hard | read_scope=area; record_metric=ns_per_op |
| stream | tier4-stream-gate | read_area_wildcard | websocket | 521.340369 | 1,000 | 1,000 | 750 | 1,333 | hard | read_scope=area; record_metric=ns_per_op |
| stream | tier4-stream-gate | read_realm_wildcard | tcp | 503.082287 | 1,000 | 1,000 | 750 | 1,333 | hard | read_scope=realm; record_metric=ns_per_op |
| stream | tier4-stream-gate | read_realm_wildcard | websocket | 540.298694 | 1,000 | 1,000 | 750 | 1,333 | hard | read_scope=realm; record_metric=ns_per_op |
| stream | tier4-stream-gate | tail_read | tcp | 430.148175 | 1,000 | 1,000 | 750 | 1,333 | hard | record_metric=ns_per_op |
| stream | tier4-stream-gate | tail_read | websocket | 477.116144 | 1,000 | 1,000 | 750 | 1,333 | hard | record_metric=ns_per_op |
| stream | tier4-stream-gate | metadata_read | tcp | 468.841919 | 1,000 | 1,000 | 750 | 1,333 | hard | record_metric=ns_per_op |
| stream | tier4-stream-gate | metadata_read | websocket | 504.573963 | 1,000 | 1,000 | 750 | 1,333 | hard | record_metric=ns_per_op |
| stream | tier4-stream-gate | unsubscribe_no_notify | tcp | 1,495.963525 | 2,500 | 400 | 1,875 | 533 | hard | record_metric=ns_per_op |
| stream | tier4-stream-gate | unsubscribe_no_notify | websocket | 1,697.50315 | 2,500 | 400 | 1,875 | 533 | hard | record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_unfiltered_baseline | tcp | 583.129688 | 1,000 | 1,000 | 750 | 1,333 | hard | read_scope=area; filter_mode=unfiltered; records_expected=100; record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_unfiltered_baseline | websocket | 621.136725 | 1,000 | 1,000 | 750 | 1,333 | hard | read_scope=area; filter_mode=unfiltered; records_expected=100; record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_unfiltered_baseline | tcp | 595.819012 | 1,000 | 1,000 | 750 | 1,333 | hard | read_scope=realm; filter_mode=unfiltered; records_expected=100; record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_unfiltered_baseline | websocket | 629.898694 | 1,000 | 1,000 | 750 | 1,333 | hard | read_scope=realm; filter_mode=unfiltered; records_expected=100; record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_all_match | tcp | 759.782819 | 1,250 | 800 | 937.5 | 1,067 | hard | read_scope=area; filter_mode=all_match; records_expected=100; record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_all_match | websocket | 786.791663 | 1,250 | 800 | 937.5 | 1,067 | hard | read_scope=area; filter_mode=all_match; records_expected=100; record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_all_match | tcp | 772.110944 | 1,250 | 800 | 937.5 | 1,067 | hard | read_scope=realm; filter_mode=all_match; records_expected=100; record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_all_match | websocket | 793.897919 | 1,250 | 800 | 937.5 | 1,067 | hard | read_scope=realm; filter_mode=all_match; records_expected=100; record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_subset_25 | tcp | 740.327344 | 1,000 | 1,000 | 750 | 1,333 | hard | read_scope=area; filter_mode=subset_25; records_expected=25; record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_subset_25 | websocket | 771.551569 | 1,250 | 800 | 937.5 | 1,067 | hard | read_scope=area; filter_mode=subset_25; records_expected=25; record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_subset_25 | tcp | 750.665369 | 1,250 | 800 | 937.5 | 1,067 | hard | read_scope=realm; filter_mode=subset_25; records_expected=25; record_metric=ns_per_op |
| stream | tier4-stream-filters | filter_subset_25 | websocket | 780.097137 | 1,250 | 800 | 937.5 | 1,067 | hard | read_scope=realm; filter_mode=subset_25; records_expected=25; record_metric=ns_per_op |

## Service Budget: Contention

All rows in this section map to `target_class = service_budget` and `budget_group = contention`.

| domain | suite | scenario | layer | current us | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| kv | tier4-kv-contention | concurrent_transactions | multiclient | 9.676429 | 28 | 35,714 | 20 | 50,000 | hard |  |
| lease | tier4-lease-contention | concurrent_acquire_release | multiclient | 9.041907 | 15 | 66,667 | 12 | 83,333 | hard |  |
| notice | tier4-notice-fanout | fanout_publish | multiclient | 2.602613 | 26 | 38,462 | 18 | 55,556 | hard | mode=delivery_confirmed |
| notice | tier4-notice-publish | publish_unacked | tcp_multipublisher | 4.645584 | 26 | 38,462 | 18 | 55,556 | hard | mode=fire_and_forget_unacked |
| notice | tier4-notice-publish | publish_unacked | websocket_multipublisher | 2.041770 | 26 | 38,462 | 18 | 55,556 | hard | mode=fire_and_forget_unacked |
| queue | tier4-queue-concurrency | concurrent_enqueues | multiclient | 46.036231 | 1,100 | 909 | 750 | 1,333 | hard |  |
| rpc | tier4-rpc-roundtrip | concurrent_requests | multiclient | 14.098315 | 26 | 38,462 | 18 | 55,556 | hard | mode=sync_concurrent |
| rpc | tier4-rpc-pipeline | request_response_pipelined | tcp_multiclient | 9.589709 | 26 | 38,462 | 18 | 55,556 | variance_gated | mode=concurrent_pipelined; full refresh rel_stddev 0.133, keep out of release gating until stable |
| rpc | tier4-rpc-pipeline | request_response_pipelined | websocket_multiclient | 5.335477 | 26 | 38,462 | 18 | 55,556 | hard | mode=concurrent_pipelined |
| schedule | tier4-schedule-concurrency | concurrent_creates | multiclient | 38.259015 | 1,700 | 588 | 1,300 | 769 | hard |  |

## Internal Explainers

All rows in this section map to `target_class = internal_explainer`. They are advisory and do not flip `product_pass`.

| domain | suite | scenario | current us | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| mailbox | tier2-subsystem-mailbox | deliver_empty_primary | 0.195792 | 0.55 | 1,818,182 | 0.45 | 2,222,222 | hard | Empty-mailbox delivery explainer for routed-domain movement. |
| mailbox | tier2-subsystem-mailbox | deliver_mid_fill_64_primary | 0.218079 | 0.55 | 1,818,182 | 0.45 | 2,222,222 | hard | Mid-fill mailbox delivery explainer. |
| scheduler | tier2-subsystem-scheduler | register_64_fresh_primary | 0.012037 | 2 | 500,000 | 1.5 | 666,667 | variance_gated | Batched fresh route registration explainer. |
| scheduler | tier2-subsystem-scheduler | register_64_replace_primary | 0.007752 | 2 | 500,000 | 1.5 | 666,667 | variance_gated | Batched replacement route registration explainer. |
| scheduler | tier2-subsystem-scheduler | register_single_fresh_primary | 0.18686 | 2 | 500,000 | 1.5 | 666,667 | variance_gated | Single fresh route registration explainer. |
| subscriptions | tier2-subsystem-subscriptions | 10k_subs_10k_matches | 81.43697 | 110 | 9,091 | 75 | 13,333 | hard | Dense subscription match explainer. |
| subscriptions | tier2-subsystem-subscriptions | insert_100_match_2 | 0.092206 | 30 | 33,333 | 20 | 50,000 | hard | Subscription insertion plus small match explainer. |
| subscriptions | tier2-subsystem-subscriptions | replace_100_patterns_then_dense_match | 0.215474 | 30 | 33,333 | 20 | 50,000 | hard | Replacement plus dense match explainer. |
| tlv_pipeline | tier2-subsystem-tlv-pipeline | decode_then_mux_route_ref_16b | 0.002949 | 10 | 100,000 | 8 | 125,000 | hard | 16-byte TLV decode and route explainer. |
| tlv_pipeline | tier2-subsystem-tlv-pipeline | decode_then_mux_route_ref_256b | 0.002958 | 10 | 100,000 | 8 | 125,000 | hard | 256-byte TLV decode and route explainer. |
| tlv_pipeline | tier2-subsystem-tlv-pipeline | decode_then_mux_route_ref_64b | 0.002955 | 10 | 100,000 | 8 | 125,000 | hard | 64-byte TLV decode and route explainer. |

## Scorecard Policy

- `engine_core_pass = true` only when every actionable engine-core target meets its operational target.
- Each `service_budget` subgroup passes independently: `direct_api`, `transport`, and `contention`.
- `product_pass = true` only when `engine_core_pass` and all three service-budget subgroup passes are true.
- Stretch attainment is reported separately and never flips primary pass/fail.

## Document History

- v2.3: Added RPC pipelined and Notice fire-and-forget rows with explicit completion-mode labels.
- v2.2: Regenerated from current `cntryl-stress.v2` rows, removed legacy criterion target keys, and added benchmark IDs to the machine-readable target mirror.
- v2.1: Introduced `engine_core`, `service_budget`, and `internal_explainer` classes, with class-aware scoreboards and hotspot selection order.
- v2.0: Added operational and stretch targets for tier3/tier4 stress suites plus selected subsystem explainers.
