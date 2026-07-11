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

## Family Actor Pools and Mailboxes

- Family actor shard count is fixed at
  `min(available_parallelism, provisioned route families)`.
- Keep the normal lane at its 16,384-message contract capacity and use the
  separate control lane for drain, cleanup, and other control work.
- Watch family-lane depth and enqueue backpressure for starvation; never
  compensate for saturation with an unbounded async queue.

## Storage and Durability Tradeoffs

- Use stronger durability levels only where required by workload.
- Keep transaction scope narrow and route-local.
- Validate fsync-backed paths with realistic I/O pressure.

## Validation

1. Confirm p95 and p99 latency do not regress.
2. Confirm error rates are unchanged or improved.
3. Confirm no cross-realm interference under load.
