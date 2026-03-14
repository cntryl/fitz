# Performance targets (single-node, best-in-class)

This document defines **numerical, testable** performance targets for Fitz on a single node. Each target has a threshold, conditions (payload, concurrency, transport), and a **verification** mapping to a benchmark scenario (existing or "new scenario required"). Use these to drive optimization and to prove competitive positioning vs SQS/RabbitMQ, Kafka, and NATS.

## Machine spec

All targets assume the same environment:

- **CPU:** 16 vCPU
- **Storage:** NVMe SSD
- **Network:** 1–10 Gbps NIC (TLS off for raw numbers)
- **RAM:** 64–128 GB
- **Build:** Release

## Reality check

Current benchmarks may not yet match every target’s conditions (e.g. 64 producers + 64 consumers for queue). This doc defines the **goal** and the **verification path** (which scenario to run or add). Implement toward these numbers; add or adjust scenarios as needed.

---

## Queue — Beat SQS & RabbitMQ

| Label | Threshold | Conditions | Verification |
|-------|-----------|------------|--------------|
| Target | Durable enqueue+reserve ≥ 500k msgs/sec | 256B payload, 64 concurrent producers + 64 consumers, acks enabled | **New scenario:** `tier3_system_queue`: 64 producers + 64 consumers, 256B payload, acks; report combined enqueue+reserve throughput. Existing `sustained_load` / `high_contention` are stepping stones. |
| Target | End-to-end latency (enqueue → reserve) p50 &lt; 500µs, p99 &lt; 2ms | Same as above | **New:** Same scenario; report latency percentiles (stress or post-process). |
| Target | High contention (1 hot queue) ≥ 200k msgs/sec sustained | Single queue, many producers/consumers | Existing: [tier3_system_queue](../../benches/tier3_system_queue.rs) scenario `high_contention`. |
| Target | Cold recovery (100k messages on restart) &lt; 250ms | 100k messages persisted, then restart and recover | Existing: [tier3_system_queue](../../benches/tier3_system_queue.rs) scenario `cold_start_recovery`. |
| Stretch | Durable enqueue+reserve ≥ 1M msgs/sec | Same conditions | Same verification scenario; higher threshold. |

---

## Stream — Beat Kafka

**Single append vs batch:** Use two modes explicitly. **Single append** = one commit/sync per message: durable, higher latency (~5–6 µs/op typical); use when latency-of-one is required. **Batch append** = batched fsync: high throughput mode; use for Kafka-class throughput (target 2M+ msgs/sec). The `sustained_append` scenario measures the single-append path; for throughput use `batch_write`.

| Label | Threshold | Conditions | Verification |
|-------|-----------|------------|--------------|
| Target | Durable append ≥ 2M msgs/sec | 256B payload, single partition–equivalent, fsync batched | Existing: [tier3_system_stream](../../benches/tier3_system_stream.rs) `sustained_append` (single-append path; for throughput use `batch_write` with batched fsync). |
| Target | Multi-area writes ≥ 1.5M msgs/sec across 8 logical streams | 256B payload | Existing: [tier3_system_stream](../../benches/tier3_system_stream.rs) `multiarea_writes`. |
| Target | Read scan ≥ 5M msgs/sec sequential, p50 &lt; 100µs for small range | Sequential scan | Existing: [tier3_system_stream](../../benches/tier3_system_stream.rs) `read_scan`. |
| Target | End-to-end append latency p50 &lt; 300µs, p99 &lt; 2ms | Same as sustained append | **New:** Same as `sustained_append` plus latency reporting. |
| Stretch | Durable append 3–5M msgs/sec | Same conditions | Same verification scenario. |

---

## RPC — Beat NATS

| Label | Threshold | Conditions | Verification |
|-------|-----------|------------|--------------|
| Target | In-proc RPC ≥ 5M req/sec, p50 &lt; 50µs, p99 &lt; 200µs | Full path: request → route → worker → response | Existing: [tier3_system_rpc](../../benches/tier3_system_rpc.rs) — ensure scenario uses full dispatch (no getters/fake work); add latency reporting if needed. |
| Target | Over TCP (loopback) ≥ 1M req/sec, p50 &lt; 300µs, p99 &lt; 1ms | Real socket, request → worker → response | Existing: [tier4_integration_rpc](../../benches/tier4_integration_rpc.rs) TCP/WS `network_roundtrip` — ensure real socket; document loopback. |
| Target | 10k inflight requests sustained ≥ 750k req/sec, no collapse | 10k concurrent in-flight RPCs | **New scenario:** tier3 or tier4 RPC with 10k in-flight requests. |
| Target | Correlation map O(1) under 100k concurrent requests; no global lock | 100k concurrent correlations | **New:** Dedicated micro-bench or stress scenario (correlation map lookup/insert). |

---

## Notice (pub/sub) — Beat NATS

| Label | Threshold | Conditions | Verification |
|-------|-----------|------------|--------------|
| Target | 1→1: ≥ 5M msgs/sec in-proc, ≥ 1M msgs/sec over TCP | One publisher, one subscriber | Existing: tier3 `sustained_fanout` (1 sub); tier4 direct + tcp. Add explicit 1→1 scenario if missing. |
| Target | 1→100 fanout: ≥ 2M deliveries/sec total, p50 &lt; 500µs | One publisher, 100 subscribers | Existing: tier3 `sustained_fanout` with 100 subs; or **new** 1→100 scenario. |
| Target | 10k subscriptions pattern match: match cost &lt; 200ns, 10k matches &lt; 200µs | 10k subscriptions, pattern match | Existing: tier2_subsystem_subscriptions / tier3 notice `pattern_matching` — align to 10k subs and match count. |
| Stretch | 100k subscriptions: linear scaling, no backtracking explosion | 100k subs | **New scenario** or document as stretch. |

---

## Schedule — World class

| Label | Threshold | Conditions | Verification |
|-------|-----------|------------|--------------|
| Target | Create ≥ 500k ops/sec in-memory, ≥ 100k durable | Schedule create (and durable path if present) | Existing: [tier3_system_schedule](../../benches/tier3_system_schedule.rs) `create_operation` (and durable path if present). |
| Target | Scan+fire O(log n): 100k timers fire cost &lt; 200µs, 1M timers &lt; 500µs | Heap-based fire; no linear scan | Existing: [tier3_system_schedule](../../benches/tier3_system_schedule.rs) `scan_fire_100` / `scan_fire_1000` / `scan_fire_10000` — add or align 100k/1M scenarios; target is per-fire cost. |
| Target | Timer accuracy ±1ms jitter under load | Under throughput load | Document or add latency jitter scenario. |
| Stretch | 1M timers loaded: predictable memory, no linear scan | 1M schedules | **New:** Memory footprint scenario or doc note. |

---

## Lease — World class locking

| Label | Threshold | Conditions | Verification |
|-------|-----------|------------|--------------|
| Target | Acquire ≥ 5M/sec in-proc, ≥ 1M over TCP | Single route (in-proc); TCP loopback | Existing: [tier3_system_lease](../../benches/tier3_system_lease.rs) single-route; [tier4_integration_lease](../../benches/tier4_integration_lease.rs) direct + tcp. |
| Target | Contention: 100 concurrent contenders, p99 acquire &lt; 2ms, no livelock | 100-way contention on one lock | **New scenario:** 100-way contention. |
| Target | Lease expiry: expire 100k leases in &lt; 200ms | Bulk expiry | **New scenario** or stretch. |
| Target | Fencing token: monotonic, lock-free or sharded | Token generation | Document or micro-bench. |

---

## Priority order (implementers)

Optimize toward targets in this order:

1. **RPC** — In-proc and TCP targets; correlation and dispatch are central.
2. **Queue** — Durable throughput and latency comparable to SQS/RabbitMQ.
3. **Stream** — Append throughput and latency comparable to Kafka.
4. **Notice** — 1→1 and 1→100 fanout for pub/sub positioning.
5. **Schedule** — Scan+fire and 1M timers for world-class positioning.
6. **Lease** — Acquire throughput and contention for coordination.

---

## Minimal architectural checklist

To reach these targets, the implementation will need:

- **Sharded mailboxes** and **sharded correlation maps** where applicable.
- **Lock striping** or per-resource locking; avoid global locks on hot paths.
- **Batching:** store and network writes batched where possible.
- **Zero-copy or minimal-copy** payload path where feasible.
- **Slab allocators / object pools** for high-allocation paths (e.g. message buffers).
- **No per-message heap allocation** on the hot path where avoidable.

Without these, throughput will likely plateau in the 200k–500k range and targets will not be met.

---

## Tier4 transport optimization (RPC + Notice)

To move TCP roundtrip from ~1–1.5 ms toward ~300–500 µs (and compete with NATS), focus on:

- **Allocations:** Minimize decode/encode allocations; reuse buffers where possible.
- **Hop count:** Request → server → route → domain actor → response → client; reduce to minimum (one decode, one route, one mailbox, one encode).
- **Task spawns / wakeups:** Avoid per-request task spawns; batch or share wakeups.
- **Syscalls / flush:** Batch writes; avoid flush-per-message where possible.

Profile the tier4 RPC and Notice TCP path first; then apply the above. Same work benefits both domains.

---

## Document history

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025 | Initial numerical targets and scenario mapping. |
