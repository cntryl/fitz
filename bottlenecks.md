# Fitz Bottlenecks

This document records the current observed performance bottlenecks by Fitz
domain. It is an optimization guide, not a domain contract. Domain meanings,
durability, replay, delivery, ownership, and session guarantees remain defined
by the development architecture docs.

## Source Context

Current RPC refresh source: `target/bench_summary.md`, generated
`2026-07-03T14:03:05Z` from `target/bench_results.json`. The Tier 3 RPC and
Tier 4 RPC rows cited below use 5 samples, are marked stable, and have
`authoritative` status in that summary.

Shared WebSocket smoke evidence for this refresh: Tier 4 KV and Tier 4 Lease
were rerun with `--runs 1 --warmup 1`; those rows are useful regression smoke
only and are marked `insufficient_data`, not authoritative.

Non-RPC domain sections below still use the previous full source:
`target/bench_summary.md`, generated `2026-07-02T20:54:57Z` from
`target/bench_results.json`. The Tier 4 stress rows cited in those sections use
5 samples, are marked stable, and have `authoritative` status in that summary.

Secondary caveat: `target/perf_proof/single_node.md`, generated
`2026-07-02T21:36:46.619565+00:00`, reports `Samples: 2, warmup: 1`. Treat that
single-node proof report as smoke-only, not proof. The run used
`FITZ_PROOF_SAMPLES=2`, so it is useful only as directional context beside the
authoritative Tier 4 summary.

## How To Read The Numbers

- `direct` is the in-process domain path and is the closest signal for core
  domain cost.
- `encoded`, `tcp`, and `websocket` add framing, transport, session, and
  roundtrip costs.
- `multiclient` includes concurrent client/session behavior and may expose
  contention that is not visible in one direct actor call.
- A low direct result points at a core-domain hot path. A high direct result
  with much lower TCP/WS results points primarily at transport/session overhead.

## Priority Order

1. Queue
2. Stream
3. Schedule
4. RPC
5. Notice
6. KV / Lease

## Queue

Queue is the clearest domain hot path bottleneck.

Evidence:

- Direct enqueue: 50,827 ops/s.
- Encoded enqueue: 52,240 ops/s.
- TCP enqueue: 6,725 ops/s.
- WebSocket enqueue: 7,402 ops/s.
- Multiclient concurrent enqueue: 6,645 ops/s.
- 64-client enqueue scaling: 3,079 ops/s.

Interpretation: direct enqueue is already low relative to KV, Lease, Notice
publish, Stream append, and RPC direct request/response. Transport then drops
end-to-end throughput into the 6.7K-7.4K ops/s range, and high client counts
drop lower. Queue optimization should start with the enqueue/write/reservation
path before spending time on generic transport tuning.

## Stream

Stream has current-path bottlenecks around concurrent appends and realm wildcard
reads.

Evidence:

- Direct append: 144,881 ops/s.
- Multiclient append: 5,216 ops/s.
- Direct exact resource read: 975,018 ops/s.
- Direct area wildcard read: 92,719 ops/s.
- Direct realm wildcard read: 5,198 ops/s.
- TCP append: 22,245 ops/s.
- WebSocket append: 24,537 ops/s.

Interpretation: exact resource reads are not the limiting path in the current
results. The concerning results are direct realm wildcard reads and concurrent
appends, both around 5.2K ops/s. Those point at core Stream storage/indexing and
concurrency behavior before transport. TCP/WS append overhead is also visible,
but it is secondary to the low direct/concurrent Stream paths.

## Schedule

Schedule is limited by per-create persistence and indexing cost, with batching
showing a clear relief path.

Evidence:

- Direct create: 12,582 ops/s.
- Multiclient creates: 12,872 ops/s.
- TCP create: 13,748 ops/s.
- WebSocket create: 15,961 ops/s.
- WebSocket batch create: 88,859 ops/s.

Interpretation: single-create throughput stays in the same class across direct,
TCP, WS, and multiclient cases, so the main limit is not transport/session
roundtrip. The WebSocket batch-create result is much higher, which suggests the
per-create persistence/indexing overhead can be amortized.

## RPC

RPC core request/worker/response coordination is no longer the dominant Tier 4
cost in the refreshed evidence. The remaining bottleneck is the one-worker
WebSocket multiclient path, which is dominated by WebSocket/socket I/O and the
serialized live worker response cycle.

Evidence:

- Direct request/response: 792,502 ops/s, 1.26 us/op.
- Encoded request/response: 777,789 ops/s, 1.29 us/op.
- Multiclient concurrent requests, 1 worker: 17,055 ops/s, 58.63 us/op.
- Multiclient concurrent requests, 4 workers: 28,908 ops/s, 34.59 us/op.
- Multiclient concurrent requests, 8 workers: 25,649 ops/s, 38.99 us/op.
- TCP request/response: 14,375 ops/s, 69.56 us/op.
- WebSocket request/response: 15,740 ops/s, 63.53 us/op.

Acceptance status: direct, encoded, and single WebSocket RPC are green. TCP
remains above the `<= 60 us/op` target, and all Tier 4 RPC multiclient rows
remain above the `<= 26 us/op` target after preserve-semantics transport and
testkit optimizations.

Profiler evidence: `target/profiles/tier4_rpc_multiclient.sample.txt` sampled a
filtered Tier 4 RPC multiclient run. The main benchmark thread was mostly parked
waiting for async work. Active requester samples clustered in
`TestWebSocketClient::recv_frame_bytes_without_timeout` through
`tokio_tungstenite`/`TcpStream::poll_read_priv`/`__recvfrom`, plus
`TestWebSocketClient::send_frame_bytes` through WebSocket write and `__sendto`.
RPC domain samples were comparatively small and were mostly in response
delivery and outbound ACK enqueueing. This points at WebSocket/socket I/O and
single-worker serialization, not the RPC codec or in-process domain core.

Attempted follow-ups: a benchmark-only blocking WebSocket client, a no-delay
ready poll before WebSocket writer flush, and a larger pipelined multiclient
measurement window were tried as local smoke optimizations and backed out
because they did not improve the retained one-worker gate.

Interpretation: direct and encoded RPC now run in the same class as the faster
in-process domain paths, so the remaining work should not target RPC codec or
core dispatch first. The stop condition is active: do not change RPC
live/ephemeral semantics, worker ACK behavior, timeout behavior, or the
one-worker default without new profiler evidence and an explicit
protocol/target decision.

## Notice

Notice publish is not the primary core bottleneck; subscription churn and
fanout paths are.

Evidence:

- Direct publish: 261,817 ops/s.
- Multiclient fanout publish: 100,865 ops/s.
- Fanout publish subscriber scaling, 1 subscriber: 8,079 ops/s.
- Fanout publish subscriber scaling, 16 subscribers: 48,723 ops/s.
- Fanout publish subscriber scaling, 64 subscribers: 91,321 ops/s.
- TCP publish: 21,409 ops/s.
- WebSocket publish: 23,415 ops/s.
- TCP subscribe/unsubscribe cycle: 12,421 ops/s.
- WebSocket subscribe/unsubscribe cycle: 14,590 ops/s.

Interpretation: direct publish is healthy compared with Queue, Schedule, and
RPC. The weaker paths are subscribe/unsubscribe churn and fanout measurement
shape, plus transport/session overhead for end-to-end publish. Keep Notice
optimization focused on live subscription management and fanout mechanics.

## KV

KV's core path is not the current bottleneck; transport/session roundtrip
dominates the observed end-to-end cost.

Evidence:

- Direct begin/put/rollback: 1,647,564 ops/s.
- Encoded begin/put/rollback: 1,277,830 ops/s.
- Multiclient concurrent transactions: 39,875 ops/s.
- TCP begin/put/rollback: 22,373 ops/s.
- WebSocket begin/put/rollback: 24,657 ops/s.

Interpretation: direct KV throughput is far ahead of every other domain in the
Tier 4 summary. The large drop from direct to TCP/WS indicates the current KV
limit is mainly transport/session roundtrip and concurrent session behavior, not
core transactional state handling.

## Lease

Lease is also primarily transport/session limited in the current results.

Evidence:

- Direct acquire/release: 251,938 ops/s.
- Multiclient acquire/release: 73,152 ops/s.
- TCP acquire/release: 24,844 ops/s.
- WebSocket acquire/release: 26,847 ops/s.

Interpretation: direct Lease throughput is strong relative to Queue, Schedule,
and RPC. The TCP/WS results cluster around 25K-27K ops/s, so Lease should stay
behind the core hot path domains unless work is specifically targeting generic
transport/session overhead.

## Current Focus

Start with domain-local work where direct or concurrent throughput is already
low: Queue enqueue, Stream realm wildcard reads and concurrent appends, and
Schedule create. For RPC, the refreshed direct/encoded rows move the remaining
target to WebSocket/session edge overhead, especially the one-worker
multiclient path. Treat Notice, KV, and Lease as edge-overhead targets unless
new benchmark evidence changes the shape of the results.
