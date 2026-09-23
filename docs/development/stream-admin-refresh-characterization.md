# Stream admin refresh characterization

This is a targeted Tier 3 characterization of the in-process Stream domain, not
a release benchmark gate. It covers clean, one-dirty-family, and
all-dirty-family admin refreshes at 4 families × 8 resources and 16 families ×
16 resources. Each resource starts with one committed event. Dirty setup
alternates a routed append-session BEGIN and ROLLBACK on existing resources,
changing the projected live-session count without growing durable history.
The fixture verifies row count, committed-event count, and previously
projected live-session count outside the timed operation. Only
`refresh_admin_snapshot_if_dirty()` is timed.

## Method and observations

Three default-profile runs were made before and after the clean-family fast
path on the same Apple M5 (10 cores, macOS aarch64), with Rust 1.98.1 and
`cntryl-stress` 0.4.0. The baseline executable built by `cargo bench --locked
--bench tier3_system_stream_admin` came from `48a55490` and was rerun
directly; the optimized runs used that `cargo bench` command at `3197f351`.
Each run has one warmup and five
measured samples per scenario. The figures below are medians across the three
runs: `ns/op` and allocations use each run's measured-sample median; p99 is
calculated from each run's recorded per-invocation latency samples, not the
stress summary's across-sample p99.

| Scenario | Families × resources | ns/op before → after | Invocation p99 ns before → after | Allocs/op before → after | Bytes/op before → after |
| --- | ---: | ---: | ---: | ---: | ---: |
| Clean | 4 × 8 | 17,023 → 102 | 64,250 → 167 | 17.24 → 0.00 | 3,236 → 0.20 |
| Clean | 16 × 16 | 29,128 → 215 | 58,708 → 500 | 67.39 → 0.02 | 13,107 → 2.36 |
| One dirty | 4 × 8 | 73,004 → 75,536 | 86,583 → 193,750 | 1,297 → 1,286 | 585,464 → 583,160 |
| One dirty | 16 × 16 | 189,111 → 201,463 | 220,291 → 481,417 | 3,239 → 3,178 | 1,164,279 → 1,152,407 |
| All dirty | 4 × 8 | 362,595 → 459,340 | 527,458 → 827,708 | 4,840 → 4,841 | 2,239,456 → 2,239,578 |
| All dirty | 16 × 16 | 3,730,820 → 3,911,337 | 4,345,709 → 5,190,750 | 48,900 → 48,916 | 17,916,940 → 17,919,221 |

The clean 16-family median fell about 135-fold, and measured allocations fell
from 67.39 to 0.02 per refresh. The optimization does **not** address dirty
family metadata scans or repeated full-set publication: all-dirty refresh
remains roughly 4 ms and 18 MB at 16 × 16. Some runs were noisy while a
separate local Rust suite used the host, so the dirty-path before/after
differences are not evidence of a regression or improvement. The allocation
diagnostic remains visible and advisory under the stress benchmark contract.

The optimized benchmark skips repeated correctness reads in the clean case's
*untimed* setup, after an initial check; it keeps checks between every dirty
refresh and after each scenario. Baseline and optimized measured operations
are identical, but this setup change is a comparison caveat. No release
benchmark IDs, baselines, performance targets, or CI workflows were changed.
