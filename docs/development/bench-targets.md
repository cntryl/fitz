# Performance Target Rubric v2.1

Fitz now tracks performance targets in two forms:

- Human-readable rubric matrix: this document
- Machine-readable mirror: [config/perf_targets.json](../../config/perf_targets.json)

The numeric thresholds remain unchanged in this refinement. This pass changes target classification, rollups, and optimization priority so Fitz can separate engine truth from transport and contention budgets.

## Baseline

All targets assume the same local single-node baseline:

- CPU: 16 vCPU
- Storage: NVMe SSD
- Network: loopback for tier4, raw numbers without TLS
- RAM: 64-128 GB
- Build: release
- Primary gate: `mean_us`

Derived `min ops/sec` is `1_000_000 / max_mean_us`. It is shown for quick scanning, but `mean_us` remains the canonical gate.

## Class Model

- `engine_core`: tier3 system-domain paths. This is the headline product truth.
- `service_budget`: tier4 product paths, split into `direct_api`, `transport`, and `contention`.
- `internal_explainer`: criterion internals that explain product movement but do not outrank product-facing misses.
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

| domain | suite | scenario | layer | metric | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
|---|---|---|---|---|---:|---:|---:|---:|---|---|
| `kv` | `tier3-system-kv` | `dual_family_concurrent` | `n/a` | `mean_us` | 1.8 | 555,556 | 1.2 | 833,333 | `hard` |  |
| `kv` | `tier3-system-kv` | `mixed_read_write_families` | `n/a` | `mean_us` | 1.3 | 769,231 | 0.9 | 1,111,111 | `hard` |  |
| `kv` | `tier3-system-kv` | `single_family_intensive` | `n/a` | `mean_us` | 15 | 66,667 | 10 | 100,000 | `hard` |  |
| `kv` | `tier3-system-kv` | `triple_family_contention` | `n/a` | `mean_us` | 1.3 | 769,231 | 0.9 | 1,111,111 | `hard` |  |
| `lease` | `tier3-system-lease` | `mixed_operations_high_load` | `n/a` | `mean_us` | 0.13 | 7,692,308 | 0.09 | 11,111,111 | `hard` |  |
| `lease` | `tier3-system-lease` | `single_route_intensive` | `n/a` | `mean_us` | 0.18 | 5,555,556 | 0.12 | 8,333,333 | `hard` |  |
| `notice` | `tier3-system-notice` | `high_subscriber_count` | `n/a` | `mean_us` | 24 | 41,667 | 16 | 62,500 | `hard` |  |
| `notice` | `tier3-system-notice` | `pattern_matching` | `n/a` | `mean_us` | 0.55 | 1,818,182 | 0.4 | 2,500,000 | `hard` |  |
| `notice` | `tier3-system-notice` | `sustained_fanout` | `n/a` | `mean_us` | 1.5 | 666,667 | 1 | 1,000,000 | `hard` |  |
| `queue` | `tier3-system-queue` | `cold_start_recovery` | `n/a` | `mean_us` | 0.65 | 1,538,462 | 0.45 | 2,222,222 | `hard` |  |
| `queue` | `tier3-system-queue` | `high_contention` | `n/a` | `mean_us` | 1.5 | 666,667 | 1 | 1,000,000 | `hard` |  |
| `queue` | `tier3-system-queue` | `mixed_workload` | `n/a` | `mean_us` | 3.4 | 294,118 | 2.5 | 400,000 | `hard` |  |
| `queue` | `tier3-system-queue` | `sustained_load` | `n/a` | `mean_us` | 1.5 | 666,667 | 1 | 1,000,000 | `hard` |  |
| `rpc` | `tier3-system-rpc` | `concurrent_tracking` | `n/a` | `mean_us` | 1.6 | 625,000 | 1.1 | 909,091 | `hard` |  |
| `rpc` | `tier3-system-rpc` | `mixed_workload` | `n/a` | `mean_us` | 3 | 333,333 | 2 | 500,000 | `hard` |  |
| `rpc` | `tier3-system-rpc` | `response_streaming` | `n/a` | `mean_us` | 1.7 | 588,235 | 1.2 | 833,333 | `hard` |  |
| `rpc` | `tier3-system-rpc` | `scaling_256` | `n/a` | `mean_us` | 7 | 142,857 | 5 | 200,000 | `hard` |  |
| `rpc` | `tier3-system-rpc` | `scaling_64` | `n/a` | `mean_us` | 2.8 | 357,143 | 2 | 500,000 | `hard` |  |
| `rpc` | `tier3-system-rpc` | `sustained_dispatch` | `n/a` | `mean_us` | 2 | 500,000 | 1.3 | 769,231 | `hard` |  |
| `schedule` | `tier3-system-schedule` | `cancel_operation` | `n/a` | `mean_us` | 1200 | 833 | 800 | 1,250 | `hard` |  |
| `schedule` | `tier3-system-schedule` | `create_operation` | `n/a` | `mean_us` | 180 | 5,556 | 120 | 8,333 | `hard` |  |
| `schedule` | `tier3-system-schedule` | `list_10` | `n/a` | `mean_us` | 0.3 | 3,333,333 | 0.2 | 5,000,000 | `hard` |  |
| `schedule` | `tier3-system-schedule` | `list_100` | `n/a` | `mean_us` | 0.2 | 5,000,000 | 0.14 | 7,142,857 | `hard` |  |
| `schedule` | `tier3-system-schedule` | `list_1000` | `n/a` | `mean_us` | 0.16 | 6,250,000 | 0.12 | 8,333,333 | `hard` |  |
| `schedule` | `tier3-system-schedule` | `mixed_workload` | `n/a` | `mean_us` | 650 | 1,538 | 450 | 2,222 | `hard` |  |
| `stream` | `tier3-system-stream` | `batch_write` | `n/a` | `mean_us` | 0.35 | 2,857,143 | 0.25 | 4,000,000 | `hard` |  |
| `stream` | `tier3-system-stream` | `multiarea_writes` | `n/a` | `mean_us` | 0.5 | 2,000,000 | 0.35 | 2,857,143 | `hard` |  |
| `stream` | `tier3-system-stream` | `offset_tracking` | `n/a` | `mean_us` | 15 | 66,667 | 10 | 100,000 | `hard` |  |
| `stream` | `tier3-system-stream` | `read_scan` | `n/a` | `mean_us` | 0.2 | 5,000,000 | 0.14 | 7,142,857 | `hard` |  |
| `stream` | `tier3-system-stream` | `sustained_append` | `n/a` | `mean_us` | 6 | 166,667 | 4 | 250,000 | `hard` |  |

## Service Budget: Direct API

All rows in this section map to `target_class = service_budget` and `budget_group = direct_api`.

| domain | suite | scenario | layer | metric | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
|---|---|---|---|---|---:|---:|---:|---:|---|---|
| `kv` | `tier4-integration-kv` | `transaction_sequence` | `direct` | `mean_us` | 25 | 40,000 | 15 | 66,667 | `hard` |  |
| `kv` | `tier4-integration-kv` | `transaction_sequence` | `encoded` | `mean_us` | 14 | 71,429 | 10 | 100,000 | `hard` |  |
| `lease` | `tier4-integration-lease` | `acquire` | `direct` | `mean_us` | 40 | 25,000 | 25 | 40,000 | `hard` |  |
| `notice` | `tier4-integration-notice` | `publish` | `direct` | `mean_us` | 6.5 | 153,846 | 4 | 250,000 | `hard` |  |
| `queue` | `tier4-integration-queue` | `enqueue` | `direct` | `mean_us` | 350 | 2,857 | 225 | 4,444 | `hard` |  |
| `queue` | `tier4-integration-queue` | `enqueue` | `encoded` | `mean_us` | 500 | 2,000 | 325 | 3,077 | `hard` |  |
| `rpc` | `tier4-integration-rpc` | `request` | `direct` | `mean_us` | 28 | 35,714 | 18 | 55,556 | `hard` |  |
| `rpc` | `tier4-integration-rpc` | `request` | `encoded` | `mean_us` | 9 | 111,111 | 6 | 166,667 | `hard` |  |
| `stream` | `tier4-integration-stream` | `append` | `direct` | `mean_us` | 8 | 125,000 | 5.5 | 181,818 | `hard` |  |

## Service Budget: Transport

All rows in this section map to `target_class = service_budget` and `budget_group = transport`.

| domain | suite | scenario | layer | metric | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
|---|---|---|---|---|---:|---:|---:|---:|---|---|
| `kv` | `tier4-integration-kv` | `network_roundtrip` | `tcp` | `mean_us` | 70 | 14,286 | 50 | 20,000 | `hard` |  |
| `kv` | `tier4-integration-kv` | `network_roundtrip` | `websocket` | `mean_us` | 110 | 9,091 | 80 | 12,500 | `hard` |  |
| `lease` | `tier4-integration-lease` | `network_roundtrip` | `tcp` | `mean_us` | 80 | 12,500 | 55 | 18,182 | `hard` |  |
| `lease` | `tier4-integration-lease` | `network_roundtrip` | `websocket` | `mean_us` | 110 | 9,091 | 75 | 13,333 | `hard` |  |
| `notice` | `tier4-integration-notice` | `network_roundtrip` | `tcp` | `mean_us` | 75 | 13,333 | 50 | 20,000 | `hard` |  |
| `notice` | `tier4-integration-notice` | `network_roundtrip` | `websocket` | `mean_us` | 70 | 14,286 | 50 | 20,000 | `hard` |  |
| `queue` | `tier4-integration-queue` | `network_roundtrip` | `tcp` | `mean_us` | 210 | 4,762 | 150 | 6,667 | `hard` |  |
| `queue` | `tier4-integration-queue` | `network_roundtrip` | `websocket` | `mean_us` | 375 | 2,667 | 250 | 4,000 | `hard` |  |
| `rpc` | `tier4-integration-rpc` | `network_roundtrip` | `tcp` | `mean_us` | 60 | 16,667 | 40 | 25,000 | `hard` |  |
| `rpc` | `tier4-integration-rpc` | `network_roundtrip` | `websocket` | `mean_us` | 160 | 6,250 | 100 | 10,000 | `hard` |  |
| `schedule` | `tier4-integration-schedule` | `network_roundtrip` | `tcp` | `mean_us` | 90 | 11,111 | 60 | 16,667 | `hard` |  |
| `schedule` | `tier4-integration-schedule` | `network_roundtrip` | `websocket` | `mean_us` | 100 | 10,000 | 70 | 14,286 | `hard` |  |
| `stream` | `tier4-integration-stream` | `network_roundtrip` | `tcp` | `mean_us` | 300 | 3,333 | 200 | 5,000 | `hard` |  |
| `stream` | `tier4-integration-stream` | `network_roundtrip` | `websocket` | `mean_us` | 220 | 4,545 | 150 | 6,667 | `hard` |  |

## Service Budget: Contention

All rows in this section map to `target_class = service_budget` and `budget_group = contention`.

| domain | suite | scenario | layer | metric | operational max us | min ops/sec | stretch max us | min ops/sec | gating | notes |
|---|---|---|---|---|---:|---:|---:|---:|---|---|
| `kv` | `tier4-integration-kv` | `concurrent_transactions` | `multiclient` | `mean_us` | 28 | 35,714 | 20 | 50,000 | `hard` |  |
| `lease` | `tier4-integration-lease` | `concurrent_clients` | `multiclient` | `mean_us` | 50 | 20,000 | 35 | 28,571 | `hard` |  |
| `notice` | `tier4-integration-notice` | `concurrent_publishers` | `multiclient` | `mean_us` | 26 | 38,462 | 18 | 55,556 | `hard` |  |
| `queue` | `tier4-integration-queue` | `concurrent_enqueues` | `multiclient` | `mean_us` | 1100 | 909 | 750 | 1,333 | `hard` |  |
| `rpc` | `tier4-integration-rpc` | `concurrent_subscribe` | `multiclient` | `mean_us` | 26 | 38,462 | 18 | 55,556 | `hard` | Bench tag still says concurrent_subscribe; benchmark behavior is concurrent requests. |
| `schedule` | `tier4-integration-schedule` | `concurrent_creates` | `multiclient` | `mean_us` | 23 | 43,478 | 16 | 62,500 | `hard` |  |
| `stream` | `tier4-integration-stream` | `concurrent_appends` | `multiclient` | `mean_us` | 50 | 20,000 | 35 | 28,571 | `hard` |  |

## Internal Explainers

All rows in this section map to `target_class = internal_explainer`. They are advisory and do not flip `product_pass`.

| benchmark | metric | operational max us | min ops/sec | stretch max us | min ops/sec | gating | max rel stddev | notes |
|---|---|---:|---:|---:|---:|---|---:|---|
| `subsystem_scheduler\spawn_different_family` | `mean_us` | 28 | 35,714 | 18 | 55,556 | `variance_gated` | 0.25 |  |
| `subsystem_scheduler\spawn_single_actor` | `mean_us` | 25 | 40,000 | 15 | 66,667 | `variance_gated` | 0.25 |  |
| `subsystem_scheduler\register_only` | `mean_us` | 2 | 500,000 | 1.5 | 666,667 | `variance_gated` | 0.25 |  |
| `subsystem_tlv_pipeline\iter_256b_64subs` | `mean_us` | 10 | 100,000 | 8 | 125,000 | `hard` | n/a |  |
| `subsystem_tlv_pipeline\iter_64b_64subs` | `mean_us` | 10 | 100,000 | 8 | 125,000 | `hard` | n/a |  |
| `subsystem_tlv_pipeline\iter_16b_64subs` | `mean_us` | 10 | 100,000 | 8 | 125,000 | `hard` | n/a |  |
| `subsystem_subscriptions\10k_subs_10k_matches` | `mean_us` | 110 | 9,091 | 75 | 13,333 | `hard` | n/a |  |
| `schedule_scan_and_fire\scan_and_fire_100_cpu_only` | `mean_us` | 650 | 1,538 | 450 | 2,222 | `hard` | n/a |  |
| `schedule_scan_and_fire\scan_and_fire_100` | `mean_us` | 700 | 1,429 | 500 | 2,000 | `hard` | n/a |  |
| `subsystem_mailbox\send_to_mailbox` | `mean_us` | 0.55 | 1,818,182 | 0.45 | 2,222,222 | `hard` | n/a |  |
| `subsystem_mailbox\send_100_messages` | `mean_us` | 50 | 20,000 | 35 | 28,571 | `hard` | n/a |  |
| `subsystem_subscriptions\insert_100_match_2` | `mean_us` | 30 | 33,333 | 20 | 50,000 | `hard` | n/a |  |

## Visibility Only

These targets remain visible in reports but are excluded from attainment percentages and hotspot selection.

| domain | suite | scenario | layer | metric | gating | notes |
|---|---|---|---|---|---|---|
| `lease` | `tier3-system-lease` | `dual_route_concurrent` | `n/a` | `mean_us` | `informational` | Current run is timer-resolution limited; keep visible until a scaled variant exists. |
| `lease` | `tier3-system-lease` | `triple_route_contention` | `n/a` | `mean_us` | `informational` | Current run is timer-resolution limited; keep visible until a scaled variant exists. |
| `schedule` | `tier3-system-schedule` | `scan_fire_100` | `n/a` | `mean_us` | `informational` | Current stress scenario is too small to gate; keep it visible until a scaled variant exists. |
| `schedule` | `tier3-system-schedule` | `scan_fire_1000` | `n/a` | `mean_us` | `informational` | Current stress scenario is too small to gate; keep it visible until a scaled variant exists. |
| `schedule` | `tier3-system-schedule` | `scan_fire_10000` | `n/a` | `mean_us` | `informational` | Current stress scenario is too small to gate; keep it visible until a scaled variant exists. |
| `schedule` | `tier4-integration-schedule` | `create` | `direct` | `mean_us` | `informational` | Current direct path is below timer resolution; keep it visible until the scenario is scaled. |

## Scorecard Policy

- `engine_core_pass = true` only when every actionable engine-core target meets its operational target.
- Each `service_budget` subgroup passes independently: `direct_api`, `transport`, and `contention`.
- `product_pass = true` only when `engine_core_pass` and all three service-budget subgroup passes are true.
- Stretch attainment is reported separately and never flips primary pass/fail.

## Document History

- v2.1: Introduced `engine_core`, `service_budget`, and `internal_explainer` classes, with class-aware scoreboards and hotspot selection order.
- v2.0: Added operational and stretch targets for tier3/tier4 stress suites plus selected tier2 criterion suites.
