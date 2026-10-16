# Domain allocation sweep, 2026-10-07

Issue: [#412](https://github.com/cntryl/fitz/issues/412).

This sweep removes allocation work from indexed wildcard suffix matching and
measures whether reusing owned KV scan keys improves validated range scans.
The improvements apply automatically. No configuration, protocol, storage,
isolation, delivery guarantee, or capacity limit changes.

## Changes

The shared subscription index uses two reusable rolling frontiers. Short route
tails fit inline; longer tails allocate the two buffers once. Matching keeps
its polynomial bound. Identical consecutive suffixes reuse the last result
within one node lookup, without retaining state between routes or nodes.

KV scans borrow the visible key while evaluating limits, then reuse its owned
buffer when the visible key is at least as large as the hidden scope prefix.
Smaller visible keys are copied so a large hidden prefix is released promptly.
The pinned storage implementation produces individually owned scan keys;
this optimization does not retain an SST block behind each response key.

## Method

Before: `12707d8b6e6ed6cf8e2a911fc1c2693ba532c4c0`, with production behavior
from `54d89a2fb70b9b7777b9dd90e24c1a5339079f35` and the new measurement fixtures.
After: `bd6327dd57d79bcfdc77d1cf2ef29976ba427098`.
Host: Apple M5, 10 cores, 24 GiB RAM, macOS arm64, rustc 1.99.0, AC power.
Release builds use the same locked dependencies and default stress profile:
one warmup, five measured samples, 500 ms duration (25 ms micro windows).
Benchmark runs are serial. Existing benchmark logging is disabled with
`FITZ_LOG_LEVEL=off OTEL_ENABLED=false`; these are measurement settings and
are not part of the optimization.

Each invocation uses:

```sh
cargo bench --quiet --locked --features benchkit --bench TARGET -- --workload WORKLOAD
```

All seven TCP lifecycle rows run once per head. The four focused workloads run
three times per head. No established baseline, target, release benchmark ID,
or sample budget is refreshed or weakened.

The indexed suffix rows intentionally specify zero allocation budgets on both
heads. The original implementation fails these budgets while validating its
matching result. Its timing trust is therefore invalid, and no qualified
micro-timing speedup is claimed. The deterministic allocation counts remain
the useful before/after evidence.

The first KV fixture exceeded the response wire ceiling. Its 64-byte keys were
corrected to 32-byte keys before the comparison baseline; 16-byte values and
1,000 rows fit the existing ceiling. The original failed fixture is retained
separately and is not used in any comparison. Every timed scan validates all
keys, values, row count, and `has_more`; fanout measurements validate receipt.

## Results and acceptance boundaries

Both indexed suffix rows eliminate heap allocation on their ordinary-route
fixtures: **2 allocations / 8 bytes to 0 / 0**, and **5 allocations / 30 bytes
to 0 / 0** for the mixed suffix. These counts agree across all three captures.
The zero-allocation budgets fail before and pass after; timing on the original
budget-failing rows is not treated as qualified performance evidence.

The first KV scan series contains noisy baseline captures (36.6% and 10.7%
relative standard deviation). Its apparent median gain is not the acceptance
claim. Supplemental comparisons execute the preserved release binaries in
clean worktrees at their original source commits, alternating A/B, B/A, A/B.
The same workloads and default profile are used, with `STRESS_SUITE` set equally
on both binaries to preserve the suite identity after copying executables.
Binary SHA256 hashes are recorded in
[the provenance file](domain-optimization-20261007-binaries.json).
No local compilation or other owned benchmarks run during these comparisons.

| Validated 1,000-row KV scan | Before rows/s | After rows/s | Gain |
| --- | ---: | ---: | ---: |
| Pair 1 | 8,628,404 | 9,212,796 | 6.8% |
| Pair 2 | 8,383,778 | 9,543,874 | 13.8% |
| Pair 3 | 8,378,633 | 9,169,146 | 9.4% |

The median paired scan gain is **9.4%**. All six paired captures have acceptable
quality, gate trust, and 0.6–2.2% relative standard deviation. This claim covers
32-byte keys, 16-byte values, 1,000 committed rows, direct actor execution, and
validation cost. It is not a TCP throughput or general storage-capacity claim.

Notice wildcard fanout has no consistent end-to-end gain: paired deltas are
+21.7%, +7.2%, and -0.6%. The unchanged Schedule exact-route control also drifts
substantially, including noisy captures. Both validate received deliveries,
but neither supports a general throughput claim. Their original three-capture
series and supplemental pairs are retained alongside the scan result.

All seven selected TCP lifecycle workloads validate completed operations before
and after. The table preserves the single-capture primary rates and quality;
these rows are correctness coverage, not universal performance qualification.
Paired latency measurements are retained in the CSV and include noisy rows.

| Domain / target | Workload | Before reported rate/s | After reported rate/s | Before / after primary quality |
| --- | --- | ---: | ---: | --- |
| Kv (`tier4_kv_gate`) | `should_measure_memory_tcp_sync_commit` | 8,338 | 7,635 | acceptable / noisy |
| Lease (`tier4_lease_gate`) | `should_measure_tcp_acquire_release_lifecycle` | 14,790 | 13,773 | acceptable / noisy |
| Notice (`tier4_notice_publish`) | `should_measure_tcp_delivery_confirmed_publish` | 22,734 | 20,537 | acceptable / noisy |
| Queue (`tier4_queue_gate`) | `should_measure_memory_tcp_queue_lifecycle` | 5,714 | 6,571 | acceptable / acceptable |
| Rpc (`tier4_rpc_roundtrip`) | `should_measure_tcp_request_response_roundtrip` | 12,527 | 15,146 | acceptable / acceptable |
| Schedule (`tier4_schedule_lifecycle`) | `should_measure_memory_tcp_delivery_confirmed_lifecycle` | 4,986 | 7,256 | noisy / acceptable |
| Stream (`tier4_stream_gate`) | `should_measure_memory_tcp_sync_write_lifecycle` | 2,141 | 4,464 | noisy / noisy |

Focused commands use these existing targets and workloads:

| Target | Workload |
| --- | --- |
| `tier1_hotpath_matcher` | `should_match_indexed` (both suffix rows) |
| `tier2_subsystem_notice` | `should_publish_double_star_suffix_16_subscribers` |
| `tier3_system_kv` | `should_scan_1000_committed_keys` |
| `tier2_subsystem_schedule_fire` | `should_publish_exact_route_100_subscribers` |

## Validation and artifacts

Before: 2,743 passed, seven existing ignored tests. After: **2,748 passed**,
seven existing ignored tests, zero failures. Formatting and strict pedantic
Clippy across all workspace targets and features pass. New tests cover legal
matching parity over more than 10,000 cases, inline and spilled frontiers,
interleaved registration/removal, scan-key ownership after rollback, and prompt
release of large hidden scope backing. Source review checks synchronous core
execution, matching bounds, isolation, deduplication, and wire/pagination limits.

[Per-capture results](domain-optimization-20261007-captures.csv) retain means,
variation, quality, trust, correctness, allocations, peak process RSS, source
commits, and benchmark identities. Full local JSON, logs, commands, and the
initial excluded fixture/probe are retained under
`/tmp/fitz-domain-sweep-results/`. The installed `cntryl-tools
summarize-benchmarks` cannot parse the stress 0.5.1 `started_at` identifier;
its failed invocation is retained as `/tmp/fitz-domain-sweep-summary.log`.
This report and CSV use the original JSON without altering it.

These native measurements do not qualify fractional-core / 512 MiB deployment
capacity, cloud providers, long soaks, or deep Queue drains. Queue's 100k drain
acceptance remains tracked separately in [#404](https://github.com/cntryl/fitz/issues/404).
No release or package publication is part of this sweep.
