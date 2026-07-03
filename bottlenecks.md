# Fitz Bottlenecks

This document records the current observed performance bottlenecks by Fitz
domain. It is an optimization guide, not a domain contract. Domain meanings,
durability, replay, delivery, ownership, and session guarantees remain defined
by the development architecture docs.

## Source Context

Current Queue, Stream, and Notice refresh source: `target/bench_summary.md`,
generated `2026-07-03T18:58:12Z` from `target/bench_results.json` after
authoritative Tier 4 reruns. The Tier 4 Queue, Stream, and Notice rows cited
below use 5 samples, are marked stable, and have `authoritative` status in that
summary.

RPC refresh source: `target/bench_summary.md`, generated
`2026-07-03T14:03:05Z` from `target/bench_results.json`. The Tier 3 RPC and
Tier 4 RPC rows cited below use 5 samples, are marked stable, and have
`authoritative` status in that summary.

Schedule, KV, and Lease sections below still use the previous full source:
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

Queue remains the first domain to validate because multiclient and client
scaling rows are still low even though current direct rows are far better than
older snapshots.

Evidence:

- Direct enqueue: 46,104 ops/s.
- Encoded enqueue: 46,300 ops/s.
- TCP enqueue: 13,579 ops/s.
- WebSocket enqueue: 16,031 ops/s.
- Multiclient concurrent enqueue: 13,719 ops/s.
- 64-client enqueue scaling: 6,376 ops/s.

Interpretation: direct and encoded enqueue are no longer as weak as the older
numbers suggested, but multiclient enqueue and especially high-client scaling
remain the practical bottlenecks. Queue work should first validate the live-count
and watch/gauge path, then profile the sink hot path only if the multiclient row
remains the worst Queue row on the next authoritative run.

## Stream

Stream no longer shows the earlier realm wildcard collapse in current
authoritative evidence. The remaining current bottlenecks are direct append
versus its old baseline and end-to-end append transport.

Evidence:

- Direct append: 177,899 ops/s.
- Multiclient append: 48,728 ops/s.
- Direct exact resource read: 1,513,487 ops/s.
- Direct area wildcard read: 145,803 ops/s.
- Direct realm wildcard read: 121,950 ops/s.
- TCP append: 24,826 ops/s.
- WebSocket append: 26,567 ops/s.

Interpretation: exact resource reads are not the limiting path, and wildcard
reads are materially healthier than the stale evidence. Direct append improved
from the cleanup pass but is still a critical regression versus the checked-in
baseline, while TCP/WS append remain around 25K-27K ops/s. Continue Stream work
only from profiler-backed append-path evidence; do not use Queue results as
Stream proof.

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

Notice direct publish improved in the current cleanup pass, but Notice still has
critical rows versus the checked-in baseline: WebSocket publish, direct publish,
and the single-subscriber fanout scaling case.

Evidence:

- Direct publish: 318,310 ops/s.
- Multiclient fanout publish: 109,436 ops/s.
- Fanout publish subscriber scaling, 1 subscriber: 11,560 ops/s.
- Fanout publish subscriber scaling, 16 subscribers: 69,869 ops/s.
- Fanout publish subscriber scaling, 64 subscribers: 95,065 ops/s.
- TCP publish: 25,584 ops/s.
- WebSocket publish: 27,987 ops/s.
- TCP subscribe/unsubscribe cycle: 14,505 ops/s.
- WebSocket subscribe/unsubscribe cycle: 16,735 ops/s.

Interpretation: the shared managed-actor dispatch cleanup helped direct publish
and fanout scaling, but it did not explain the WebSocket publish gap. Keep
Notice work focused on transport/session publish overhead and subscription
matching/fanout churn; do not bypass the managed actor or change ephemeral
fanout semantics to chase the old baseline.

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

Start with domain-local work where current authoritative evidence is still weak:
Queue multiclient enqueue and high-client scaling, Stream append recovery, and
Schedule create once it is rerun authoritatively. For Notice, the remaining
critical signal is mostly WebSocket/session publish plus fanout churn. For RPC,
the refreshed direct/encoded rows move the remaining target to WebSocket/session
edge overhead, especially the one-worker multiclient path. Treat KV and Lease as
edge-overhead targets unless new benchmark evidence changes the shape of the
results.
