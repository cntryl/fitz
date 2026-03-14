# Performance Tuning

Tune Fitz with measurements, not guesses.

## Tuning Order

1. Measure baseline throughput and latency.
2. Identify bottleneck class: CPU, memory, storage, or network.
3. Change one variable at a time.
4. Re-run representative benchmarks.

Benchmark references are in [development/benchmarks.md](../development/benchmarks.md) and [development/bench-targets.md](../development/bench-targets.md).

## Runtime Hot Path Guidance

- Keep domain handlers synchronous and short.
- Avoid allocation-heavy transformations in codec and routing path.
- Keep wildcard subscription patterns bounded per realm.

## Scheduler and Mailbox

- Ensure scheduler worker count matches available cores.
- Watch mailbox depth trends for starvation.
- Prioritize control-plane traffic during degradation.

## Storage and Durability Tradeoffs

- Use stronger durability levels only where required by workload.
- Keep transaction scope narrow and route-local.
- Validate fsync-backed paths with realistic I/O pressure.

## Validation

1. Confirm p95 and p99 latency do not regress.
2. Confirm error rates are unchanged or improved.
3. Confirm no cross-realm interference under load.
