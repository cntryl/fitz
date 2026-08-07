# Performance Target Rubric v2.4

Fitz keeps its machine-readable performance targets in
[`config/perf_targets.json`](../../config/perf_targets.json). This document is
the human-readable mirror of the 14 primary Tier 4 throughput rows in
[`config/bench_release_ids.txt`](../../config/bench_release_ids.txt).

## Baseline

These current values come from the clean default-profile run generated on
2026-08-07:

- CPU: 12th Gen Intel Core i9-12900HK, 20 logical cores
- RAM: 62 GiB
- OS: Linux x86_64
- Network: loopback, without TLS
- Build: release
- Release storage: memory-backed KV and Stream wire lifecycle gates
- Storage coverage: local-disk rows remain deep `storage_characterization`
  results
- Primary comparison metric: mean microseconds per completed operation

Throughput records are normalized to `mean_us` as
`1_000_000 / operations_per_second`. Lower values are better. The derived
minimum operations per second columns are retained for quick scanning, while
`mean_us` remains canonical.

The KV and Stream thresholds were reset around their newly promoted
memory-backed rows: operational maximum is 20% above the current mean and
stretch maximum is 10% above it. The other domain thresholds remain the
previously reviewed operational goals, with only their current measurements
refreshed.

## Release Regression Gates

All rows use `target_class = regression_gate`, `budget_group =
tier4_integration`, and hard gating.

| domain | suite | scenario | layer | current us | operational max us | min ops/sec | stretch max us | min ops/sec |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| kv | tier4-kv-gate | begin_put_commit_lifecycle | tcp | 83.149 | 99.779 | 10,022 | 91.464 | 10,933 |
| kv | tier4-kv-gate | begin_put_commit_lifecycle | websocket | 81.590 | 97.908 | 10,213 | 89.749 | 11,142 |
| lease | tier4-lease-gate | acquire_release_lifecycle | tcp | 49.409 | 73.740 | 13,018 | 67.590 | 14,645 |
| lease | tier4-lease-gate | acquire_release_lifecycle | websocket | 48.758 | 80.040 | 11,992 | 73.370 | 13,492 |
| notice | tier4-notice-publish | delivery_confirmed_publish | tcp | 28.813 | 47.000 | 20,421 | 43.090 | 22,974 |
| notice | tier4-notice-publish | delivery_confirmed_publish | websocket | 24.721 | 45.770 | 20,973 | 41.950 | 23,595 |
| queue | tier4-queue-gate | enqueue_reserve_ack_lifecycle | tcp | 94.530 | 308.940 | 3,107 | 283.190 | 3,495 |
| queue | tier4-queue-gate | enqueue_reserve_ack_lifecycle | websocket | 99.158 | 216.570 | 4,432 | 198.520 | 4,986 |
| rpc | tier4-rpc-roundtrip | request_response_roundtrip | tcp | 50.952 | 60.300 | 15,919 | 55.270 | 17,909 |
| rpc | tier4-rpc-roundtrip | request_response_roundtrip | websocket | 50.321 | 61.480 | 15,613 | 56.360 | 17,565 |
| schedule | tier4-schedule-lifecycle | create_fire_ack_lifecycle | tcp | 69.762 | 130.690 | 7,345 | 119.800 | 8,263 |
| schedule | tier4-schedule-lifecycle | create_fire_ack_lifecycle | websocket | 71.942 | 132.570 | 7,241 | 121.520 | 8,146 |
| stream | tier4-stream-gate | sync_write_lifecycle | tcp | 153.630 | 184.356 | 5,424 | 168.993 | 5,917 |
| stream | tier4-stream-gate | sync_write_lifecycle | websocket | 150.158 | 180.190 | 5,549 | 165.174 | 6,054 |

## Interpretation

- Release acceptance uses primary throughput records only. Paired latency
  records remain diagnostic because their per-operation distributions are
  intentionally noisier than the duration-window throughput signal.
- Local-disk KV, Stream, Queue, and Schedule rows still appear in the full
  benchmark report. A noisy storage characterization row is a review signal,
  not a release-manifest failure.
- Add a workload to this rubric only after it is present in a clean full
  baseline, stable under the default profile, and intentionally promoted to the
  release manifest.

## Document History

| Date | Version | Changes |
| --- | --- | --- |
| 2026-08-07 | 2.4 | Rebuilt the rubric from the clean 14-row release manifest, removed stale workload expectations, and promoted memory-backed KV and Stream transport gates. |
| 2026-07-07 | 2.3 | Previous target rubric. |
