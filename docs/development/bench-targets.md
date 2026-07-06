# Performance Target Rubric v2.2

Fitz tracks performance targets in two forms:

- Human-readable rubric matrix: this document
- Machine-readable mirror: [config/perf_targets.json](../../config/perf_targets.json)

This version is generated from current `cntryl-stress.v2` artifacts. Current values are derived from each selected row's `throughput_ops_per_s` as `1_000_000 / ops_per_second`; `mean_us` remains the canonical target metric.

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
| kv | tier3-system-kv | dual_family_concurrent | 0.114364 | 1.8 | 555,556 | 1.2 | 833,333 | hard |  |
| kv | tier3-system-kv | mixed_read_write_families | 0.092737 | 1.3 | 769,231 | 0.9 | 1,111,111 | hard |  |
| kv | tier3-system-kv | single_family_intensive | 0.256644 | 15 | 66,667 | 10 | 100,000 | hard |  |
| kv | tier3-system-kv | triple_family_contention | 0.099923 | 1.3 | 769,231 | 0.9 | 1,111,111 | hard |  |
| lease | tier3-system-lease | dual_route_concurrent | 4.397957 | 1.8 | 555,556 | 1.3 | 769,231 | hard |  |
| lease | tier3-system-lease | mixed_operations_high_load | 1.793395 | 1.3 | 769,231 | 1 | 1,000,000 | hard |  |
| lease | tier3-system-lease | single_route_intensive | 4.468304 | 2.2 | 454,545 | 1.6 | 625,000 | hard |  |
| lease | tier3-system-lease | triple_route_contention | 0.504909 | 1.05 | 952,381 | 0.8 | 1,250,000 | hard |  |
| notice | tier3-system-notice | high_subscriber_count | 7.87736 | 24 | 41,667 | 16 | 62,500 | hard |  |
| notice | tier3-system-notice | pattern_matching | 0.55388 | 0.55 | 1,818,182 | 0.4 | 2,500,000 | hard |  |
| notice | tier3-system-notice | sustained_fanout | 0.55656 | 1.5 | 666,667 | 1 | 1,000,000 | hard |  |
| queue | tier3-system-queue | bulk_recovery | 0.259956 | 0.65 | 1,538,462 | 0.45 | 2,222,222 | hard |  |
| queue | tier3-system-queue | high_contention | 0.928709 | 1.5 | 666,667 | 1 | 1,000,000 | hard |  |
| queue | tier3-system-queue | mixed_steady_state | 2.004491 | 3.4 | 294,118 | 2.5 | 400,000 | hard |  |
| queue | tier3-system-queue | sustained_load | 0.932794 | 1.5 | 666,667 | 1 | 1,000,000 | hard |  |
| rpc | tier3-system-rpc | scaling_256_full_roundtrip | 0.736571 | 7 | 142,857 | 5 | 200,000 | hard |  |
| rpc | tier3-system-rpc | scaling_64_full_roundtrip | 0.736014 | 2.8 | 357,143 | 2 | 500,000 | hard |  |
| rpc | tier3-system-rpc | short_roundtrip_batch | 0.739617 | 3 | 333,333 | 2 | 500,000 | hard |  |
| rpc | tier3-system-rpc | single_response_throughput | 0.73194 | 1.7 | 588,235 | 1.2 | 833,333 | hard |  |
| rpc | tier3-system-rpc | steady_state_tracking | 0.732075 | 1.6 | 625,000 | 1.1 | 909,091 | hard |  |
| rpc | tier3-system-rpc | sustained_dispatch | 0.732844 | 2 | 500,000 | 1.3 | 769,231 | hard |  |
| schedule | tier3-system-schedule | collect_due_occurrences_not_ready_1000 | 0.038092 | 0.075 | 13,333,333 | 0.055 | 18,181,818 | hard |  |
| schedule | tier3-system-schedule | collect_due_occurrences_partial_ready_1000 | 5.433381 | 1.4 | 714,286 | 1 | 1,000,000 | hard |  |
| schedule | tier3-system-schedule | list_uncached_9_of_10 | 0.005255 | 0.015 | 66,666,667 | 0.01 | 100,000,000 | hard |  |
| schedule | tier3-system-schedule | list_uncached_99_of_100 | 0.00189 | 0.008 | 125,000,000 | 0.006 | 166,666,667 | hard |  |
| schedule | tier3-system-schedule | list_uncached_999_of_1000 | 0.001451 | 0.0075 | 133,333,333 | 0.0055 | 181,818,182 | hard |  |
| stream | tier3-system-stream | batch_write | 0.243857 | 1.538 | 650,195 | 1.25 | 800,000 | hard |  |
| stream | tier3-system-stream | multiarea_writes | 0.247616 | 1.538 | 650,195 | 1.25 | 800,000 | hard |  |
| stream | tier3-system-stream | offset_tracking | 4.629945 | 6.667 | 149,993 | 5 | 200,000 | hard |  |
| stream | tier3-system-stream | publish_fanout | 45.677448 | 1,818.182 | 550 | 1,538.462 | 650 | hard |  |
| stream | tier3-system-stream | read_area_wildcard | 0.12498 | 1.538 | 650,195 | 1.25 | 800,000 | hard |  |
| stream | tier3-system-stream | read_realm_wildcard | 0.155917 | 1.6 | 625,000 | 1.333 | 750,188 | hard |  |
| stream | tier3-system-stream | read_scan | 0.036538 | 1.176 | 850,340 | 1 | 1,000,000 | hard |  |
| stream | tier3-system-stream | sustained_append | 0.262375 | 2 | 500,000 | 1.538 | 650,195 | hard |  |

## Service Budget: Direct API

All rows in this section map to `target_class = service_budget` and `budget_group = direct_api`.

| domain | suite | scenario | layer | current us | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| kv | tier4-integration-kv | transaction_sequence | direct | 0.46111 | 25 | 40,000 | 15 | 66,667 | hard |  |
| kv | tier4-integration-kv | transaction_sequence | encoded | 0.610435 | 14 | 71,429 | 10 | 100,000 | hard |  |
| lease | tier4-integration-lease | acquire_release | direct | 4.141907 | 2.5 | 400,000 | 1.8 | 555,556 | hard |  |
| notice | tier4-integration-notice | publish | direct | 0.535597 | 6.5 | 153,846 | 4 | 250,000 | hard |  |
| queue | tier4-integration-queue | enqueue | direct | 15.449946 | 350 | 2,857 | 225 | 4,444 | hard |  |
| queue | tier4-integration-queue | enqueue | encoded | 15.007234 | 500 | 2,000 | 325 | 3,077 | hard |  |
| rpc | tier4-integration-rpc | request_response | direct | 0.677891 | 28 | 35,714 | 18 | 55,556 | hard |  |
| rpc | tier4-integration-rpc | request_response | encoded | 0.702622 | 9 | 111,111 | 6 | 166,667 | hard |  |
| schedule | tier4-integration-schedule | create | direct | 64.392909 | 1,700 | 588 | 1,300 | 769 | hard |  |
| stream | tier4-integration-stream | append | direct | 0.28487 | 2 | 500,000 | 1.538 | 650,195 | hard |  |
| stream | tier4-integration-stream | read_area_wildcard | direct | 0.117269 | 3.077 | 324,992 | 2.5 | 400,000 | hard |  |
| stream | tier4-integration-stream | read_realm_wildcard | direct | 0.165027 | 5 | 200,000 | 3.846 | 260,010 | hard |  |
| stream | tier4-integration-stream | read_resource_exact | direct | 0.104707 | 2.5 | 400,000 | 2 | 500,000 | hard |  |

## Service Budget: Transport

All rows in this section map to `target_class = service_budget` and `budget_group = transport`.

| domain | suite | scenario | layer | current us | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| kv | tier4-integration-kv | transaction_sequence | tcp | 34.482934 | 70 | 14,286 | 50 | 20,000 | hard |  |
| kv | tier4-integration-kv | transaction_sequence | websocket | 35.453198 | 110 | 9,091 | 80 | 12,500 | hard |  |
| lease | tier4-integration-lease | acquire_release | tcp | 29.197522 | 55 | 18,182 | 40 | 25,000 | hard |  |
| lease | tier4-integration-lease | acquire_release | websocket | 31.176991 | 55 | 18,182 | 40 | 25,000 | hard |  |
| notice | tier4-integration-notice | publish | tcp | 6.141124 | 75 | 13,333 | 50 | 20,000 | hard |  |
| notice | tier4-integration-notice | publish | websocket | 3.906868 | 70 | 14,286 | 50 | 20,000 | hard |  |
| queue | tier4-integration-queue | enqueue | tcp | 54.164346 | 210 | 4,762 | 150 | 6,667 | hard |  |
| queue | tier4-integration-queue | enqueue | websocket | 51.967565 | 375 | 2,667 | 250 | 4,000 | hard |  |
| rpc | tier4-integration-rpc | request_response | tcp | 44.173859 | 60 | 16,667 | 40 | 25,000 | hard |  |
| rpc | tier4-integration-rpc | request_response | websocket | 45.322905 | 160 | 6,250 | 100 | 10,000 | hard |  |
| schedule | tier4-integration-schedule | batch_create | websocket | 4.904397 | 60 | 16,667 | 45 | 22,222 | hard |  |
| schedule | tier4-integration-schedule | create | tcp | 52.219679 | 1,700 | 588 | 1,300 | 769 | hard |  |
| schedule | tier4-integration-schedule | create | websocket | 50.680186 | 1,700 | 588 | 1,300 | 769 | hard |  |
| stream | tier4-integration-stream | append | tcp | 22.411703 | 83.333 | 12,000 | 62.5 | 16,000 | hard |  |
| stream | tier4-integration-stream | append | websocket | 21.985477 | 83.333 | 12,000 | 62.5 | 16,000 | hard |  |
| stream | tier4-integration-stream | read_area_wildcard | tcp | 0.487458 | 5.556 | 179,986 | 4.167 | 239,981 | hard |  |
| stream | tier4-integration-stream | read_area_wildcard | websocket | 0.485009 | 5.556 | 179,986 | 4.444 | 225,023 | hard |  |
| stream | tier4-integration-stream | read_realm_wildcard | tcp | 0.504052 | 5.556 | 179,986 | 4.348 | 229,991 | hard |  |
| stream | tier4-integration-stream | read_realm_wildcard | websocket | 0.50177 | 5.556 | 179,986 | 4.545 | 220,022 | hard |  |
| stream | tier4-integration-stream | read_resource_exact | tcp | 0.449381 | 2.857 | 350,018 | 2.222 | 450,045 | hard |  |
| stream | tier4-integration-stream | read_resource_exact | websocket | 0.438838 | 3.333 | 300,030 | 2.5 | 400,000 | hard |  |

## Service Budget: Contention

All rows in this section map to `target_class = service_budget` and `budget_group = contention`.

| domain | suite | scenario | layer | current us | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| kv | tier4-integration-kv | concurrent_transactions | multiclient | 9.637535 | 28 | 35,714 | 20 | 50,000 | hard |  |
| lease | tier4-integration-lease | concurrent_acquire_release | multiclient | 9.00077 | 15 | 66,667 | 12 | 83,333 | hard |  |
| notice | tier4-integration-notice | fanout_publish | multiclient | 2.351483 | 26 | 38,462 | 18 | 55,556 | hard |  |
| queue | tier4-integration-queue | concurrent_enqueues | multiclient | 44.684344 | 1,100 | 909 | 750 | 1,333 | hard |  |
| rpc | tier4-integration-rpc | concurrent_requests | multiclient | 14.118149 | 26 | 38,462 | 18 | 55,556 | hard |  |
| schedule | tier4-integration-schedule | concurrent_creates | multiclient | 37.654803 | 1,700 | 588 | 1,300 | 769 | hard |  |
| stream | tier4-integration-stream | concurrent_appends | multiclient | 7.990047 | 45.455 | 22,000 | 33.333 | 30,000 | hard |  |

## Internal Explainers

All rows in this section map to `target_class = internal_explainer`. They are advisory and do not flip `product_pass`.

| domain | suite | scenario | current us | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| mailbox | tier2-subsystem-mailbox | deliver_empty_primary | 0.166848 | 0.55 | 1,818,182 | 0.45 | 2,222,222 | hard | Empty-mailbox delivery explainer for routed-domain movement. |
| mailbox | tier2-subsystem-mailbox | deliver_mid_fill_64_primary | 0.228301 | 0.55 | 1,818,182 | 0.45 | 2,222,222 | hard | Mid-fill mailbox delivery explainer. |
| scheduler | tier2-subsystem-scheduler | register_64_fresh_primary | 0.012192 | 2 | 500,000 | 1.5 | 666,667 | variance_gated | Batched fresh route registration explainer. |
| scheduler | tier2-subsystem-scheduler | register_64_replace_primary | 0.007722 | 2 | 500,000 | 1.5 | 666,667 | variance_gated | Batched replacement route registration explainer. |
| scheduler | tier2-subsystem-scheduler | register_single_fresh_primary | 0.184376 | 2 | 500,000 | 1.5 | 666,667 | variance_gated | Single fresh route registration explainer. |
| subscriptions | tier2-subsystem-subscriptions | 10k_subs_10k_matches | 82.434017 | 110 | 9,091 | 75 | 13,333 | hard | Dense subscription match explainer. |
| subscriptions | tier2-subsystem-subscriptions | insert_100_match_2 | 0.099953 | 30 | 33,333 | 20 | 50,000 | hard | Subscription insertion plus small match explainer. |
| subscriptions | tier2-subsystem-subscriptions | replace_100_patterns_then_dense_match | 0.212617 | 30 | 33,333 | 20 | 50,000 | hard | Replacement plus dense match explainer. |
| tlv_pipeline | tier2-subsystem-tlv-pipeline | decode_then_mux_route_ref_16b | 0.003051 | 10 | 100,000 | 8 | 125,000 | hard | 16-byte TLV decode and route explainer. |
| tlv_pipeline | tier2-subsystem-tlv-pipeline | decode_then_mux_route_ref_256b | 0.003052 | 10 | 100,000 | 8 | 125,000 | hard | 256-byte TLV decode and route explainer. |
| tlv_pipeline | tier2-subsystem-tlv-pipeline | decode_then_mux_route_ref_64b | 0.003038 | 10 | 100,000 | 8 | 125,000 | hard | 64-byte TLV decode and route explainer. |

## Scorecard Policy

- `engine_core_pass = true` only when every actionable engine-core target meets its operational target.
- Each `service_budget` subgroup passes independently: `direct_api`, `transport`, and `contention`.
- `product_pass = true` only when `engine_core_pass` and all three service-budget subgroup passes are true.
- Stretch attainment is reported separately and never flips primary pass/fail.

## Document History

- v2.2: Regenerated from current `cntryl-stress.v2` rows, removed legacy criterion target keys, and added benchmark IDs to the machine-readable target mirror.
- v2.1: Introduced `engine_core`, `service_budget`, and `internal_explainer` classes, with class-aware scoreboards and hotspot selection order.
- v2.0: Added operational and stretch targets for tier3/tier4 stress suites plus selected subsystem explainers.
