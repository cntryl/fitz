# Fitz Production Credibility Checklist

This document turns the current benchmark inventory, perf targets, and repo notes into a concrete validation checklist for production workloads.

Use it as a work queue for benchmarks and stress tests. The numbers below are the current Fitz operational targets from [bench-targets.md](bench-targets.md) and `config/perf_targets.json`, normalized to the 16-vCPU baseline used by the perf rubric. They are starting points to verify, not guarantees of parity with NATS, Redis, Kafka, RabbitMQ, or DynamoDB.

## Cross-Domain Questions

- Do we have p50, p95, and p99 latency under sustained load for each domain, not just mean time?
- Do we know memory cost per active resource: transaction, subscription, lease, schedule, queue item, stream session, or pending RPC?
- Do we know the knee where throughput stops scaling linearly as clients, subscribers, workers, or routes increase?
- Do we know what error signal each client should receive before the system reaches collapse?
- Do we know whether the hotspot is CPU, memory, lock contention, storage, or network for each domain?

## DOMAIN: KV

Performance characteristics:
- Current targets to verify: `single_family_intensive` at 66,667 ops/sec and `mixed_read_write_families` / `dual_family_concurrent` / `triple_family_contention` at 769,231 ops/sec on the 16-vCPU baseline.
- Measure p50, p95, and p99 for hot-key updates, wide keyspace reads, and prefix scans.
- Measure memory cost per active transaction and per hot prefix entry.
- Measure connection cost per client session plus the per-session transaction map.

Scaling cliffs to discover:
- Same-key writes should show a clear knee once storage lock contention dominates; verify the exact client count where retries or queueing begins.
- Prefix scans should degrade once keys per prefix move from small to wide fanout; verify the knee at 10x and 100x prefix growth.
- Large transactions should show a different slope than small transactions; verify the point where batch size changes from CPU bound to storage bound.

Contention questions:
- What happens when many clients hit the same key or the same route family?
- What happens when many readers and writers target the same prefix?
- Does one hot family interfere with otherwise cold families?

Workload shapes to benchmark:
- Hot key updates.
- Wide keyspace mixed read/write.
- Prefix scans.
- Batch writes versus single writes.
- Few large transactions versus many small transactions.

Failure mode questions:
- What happens if the storage engine restarts during write-heavy load?
- What happens if clients retry the same transaction after timeout?
- What happens if partial message delivery occurs on the transport boundary?

Resource limit questions:
- What is the max safe active transaction count per session?
- What is the max active route-family count before the router or storage layer degrades?
- What is the max prefix cardinality before scans become unacceptable?

Client behavior questions:
- Do connect/disconnect loops increase stale transaction state?
- Do abandoned transactions leak memory or mailbox capacity?
- Do retry storms amplify contention on hot keys?

Backpressure questions:
- Does Fitz reject new writes when the mailbox or storage path is saturated?
- Do clients get an explicit busy or mailbox-full signal, or only a timeout?
- Is there a clear retry contract for conflict or saturation?

Latency sensitivity:
- Mostly CPU and storage bound.
- Tail latency should be expected on hot-key writes and deep prefix scans.
- Lock contention is the first thing to test when p99 diverges from p50.

Realistic expectations to validate:
- Starting operational target: 66,667 to 769,231 ops/sec aggregate on the current benchmark baseline, depending on family contention.
- Rough 16-vCPU normalized equivalent: about 4,167 to 48,077 ops/sec/core if the workload scales linearly.
- External comparison target: DynamoDB-style hot-key behavior should not collapse earlier than the current KV contention tests.

Benchmarks required:
- Hot key update storm.
- Mixed read/write across multiple families.
- Prefix scan growth curve.
- Large transaction sweep.
- Transaction retry storm.

Red flag:
- If p99 blows up on a hot key or consistency drifts under contention, developers will not trust KV.

## DOMAIN: STREAM

Performance characteristics:
- Current targets to verify: `sustained_append` at 166,667 ops/sec, `batch_write` at 2,857,143 ops/sec, `multiarea_writes` at 2,000,000 ops/sec, `read_scan` at 5,000,000 ops/sec, and `offset_tracking` at 66,667 ops/sec.
- Measure p50, p95, and p99 for append, replay, and commit-triggered notification paths.
- Measure memory per active stream session, per replay cursor, and per buffered append batch.
- Measure connection cost per stream writer and per reader.

Scaling cliffs to discover:
- A second writer to the same resource should hit the single-active-session cliff immediately; verify the exact failure mode and latency.
- Replay should slow down as backlog grows; verify the knee where catch-up becomes disk bound or memory bound.
- Lease minting and commit fanout should show a burst cliff when many streams become due at once.

Contention questions:
- What happens when many writers append to the same stream?
- What happens when many readers replay the same backlog concurrently?
- What happens when many streams in one realm commit at the same time?

Workload shapes to benchmark:
- Append-heavy writers.
- Replay-heavy readers.
- Small streams versus few very large streams.
- Read-during-append.
- Large append payloads.
- Batch write bursts.

Failure mode questions:
- What happens if the writer disconnects between append and commit?
- What happens if the process restarts while there are buffered appends?
- What happens if notifications are delayed or partially delivered?

Resource limit questions:
- What is the max stream length before replay becomes operationally unacceptable?
- What is the max active session count per resource?
- What is the max batch size before commit limit enforcement kicks in?

Client behavior questions:
- Do reconnect loops leave stale active sessions behind?
- Do clients that never commit create unbounded buffered state?
- Do replay clients that lag behind create retention pressure?

Backpressure questions:
- Does Fitz reject a second active writer clearly and early?
- Does it throttle commit fanout or simply queue until memory grows?
- Does a slow reader block unrelated writers?

Latency sensitivity:
- CPU bound on offset bookkeeping and commit handling.
- Memory bound on replay backlog and buffered appends.
- Network bound only once replay or fanout is transport dominated.

Realistic expectations to validate:
- Starting operational target: 166,667 to 5,000,000 ops/sec aggregate across the current baseline, depending on whether the path is append, scan, or offset tracking.
- Rough 16-vCPU normalized equivalent: about 10,417 to 312,500 ops/sec/core if the path scales linearly.
- External comparison target: Kafka-style append and replay should stay stable under backlog growth, not just under ideal append-only load.

Benchmarks required:
- Append storm.
- Replay catch-up stress.
- Large payload append sweep.
- Single-writer versus multi-writer conflict.
- Commit fanout under many streams.

Red flag:
- If replay time grows faster than backlog or a second writer can sneak through, stream trust is broken.

## DOMAIN: QUEUE

Performance characteristics:
- Current targets to verify: `sustained_load` at 666,667 ops/sec, `high_contention` at 666,667 ops/sec, `mixed_workload` at 294,118 ops/sec, and `cold_start_recovery` at 1,538,462 ops/sec.
- Measure p50, p95, and p99 for enqueue, dequeue, ack, and redelivery.
- Measure memory cost per queued message and per in-flight lease.
- Measure connection cost per producer and consumer.

Scaling cliffs to discover:
- Queue depth should reveal a clear knee around the current capacity policy; verify the exact depth where latency increases sharply.
- Many workers on the same queue should expose contention on dequeue and ack state.
- Redelivery should show a visible cliff once slow consumers create lease expiry pressure.

Contention questions:
- What happens when many producers hit the same queue?
- What happens when many consumers pull from the same queue?
- What happens when a slow consumer shares the queue with fast consumers?

Workload shapes to benchmark:
- Small jobs versus large jobs.
- Slow consumers.
- Batch dequeue and batch ack.
- Redelivery pressure.
- Cold start recovery.

Failure mode questions:
- What happens if a worker dies with leased messages?
- What happens if a consumer never acks?
- What happens if the broker restarts with a deep queue and many inflight leases?

Resource limit questions:
- What is the max queue depth before latency and memory become unacceptable?
- What is the max inflight lease count per queue?
- What is the max redelivery count before poison-message handling is required?

Client behavior questions:
- Do aggressive polling clients starve normal producers?
- Do never-ack clients cause visible redelivery storms?
- Do connect/disconnect loops amplify queue churn?

Backpressure questions:
- Does Fitz stop accepting new messages near the depth cap?
- Does it signal queue-full, slow-consumer, or lease-expired states clearly?
- Is there a distinct signal for producer saturation versus consumer lag?

Latency sensitivity:
- Memory and lock bound once the queue gets deep.
- Tail latency is expected to grow with lease expiry and redelivery.
- Storage matters during recovery and durability-heavy paths.

Realistic expectations to validate:
- Starting operational target: 294,118 to 1,538,462 ops/sec aggregate on the current baseline, depending on workload shape.
- Rough 16-vCPU normalized equivalent: about 18,382 to 96,154 ops/sec/core if the path scales linearly.
- External comparison target: RabbitMQ-style work queues should remain stable under slow consumers and redelivery pressure.

Benchmarks required:
- Queue worker scaling test.
- Slow consumer test.
- Redelivery storm test.
- Queue capacity sweep.
- Cold start recovery test.

Red flag:
- If queue depth makes p99 explode or redelivery correctness is unclear, queue adoption will stall.

## DOMAIN: NOTICE

Performance characteristics:
- Current targets to verify: `sustained_fanout` at 666,667 ops/sec, `high_subscriber_count` at 41,667 ops/sec, and `pattern_matching` at 1,818,182 ops/sec.
- Measure p50, p95, and p99 for publish fanout, subscriber churn, and wildcard match cost.
- Measure memory cost per active subscription and per wildcard pattern.
- Measure connection cost per subscriber.

Scaling cliffs to discover:
- Fanout should show a knee once subscriber count reaches the hundreds and then the thousands; verify the exact breakpoints for 100, 1,000, and 10,000 subscribers.
- Wildcard pattern explosion should change the match curve when `*` and `**` patterns dominate the trie.
- Session cleanup should show O(N) behavior when a session owns many subscriptions.

Contention questions:
- What happens when many clients publish the same route?
- What happens when many subscribers listen to the same route or pattern?
- What happens when a single slow subscriber shares a fanout set with fast subscribers?

Workload shapes to benchmark:
- Broadcast storms.
- Subscriber churn.
- Reconnect storms.
- Deep wildcard patterns.
- Dense versus sparse pattern matching.

Failure mode questions:
- What happens when publish fanout collides with subscriber disconnects?
- What happens when one subscriber mailbox is full during a broadcast?
- What happens if a reconnect storm arrives while fanout is in progress?

Resource limit questions:
- What is the max subscription count per realm?
- What is the max wildcard pattern count before matching becomes too expensive?
- What is the max fanout set size before publish latency becomes user-visible?

Client behavior questions:
- Do subscribe/unsubscribe loops fragment the index?
- Do stale subscribers remain in the fanout path?
- Do clients that pin one route create a hot partition?

Backpressure questions:
- Does Fitz reject a publish when one destination mailbox is full, or does it shed work earlier?
- What signal does a publisher receive when fanout is saturated?
- Is there any protection against slow-subscriber amplification?

Latency sensitivity:
- CPU bound on match and fanout.
- Network bound once delivery fanout becomes large.
- Lock contention appears when subscription churn overlaps with publish load.

Realistic expectations to validate:
- Starting operational target: 41,667 to 666,667 ops/sec aggregate on the current baseline, depending on subscriber count and pattern shape.
- Rough 16-vCPU normalized equivalent: about 2,604 to 41,667 ops/sec/core if the path scales linearly.
- External comparison target: NATS-style broadcast latency should stay flat enough that p99 does not balloon with subscriber count.

Benchmarks required:
- Notice broadcast test.
- Subscriber churn test.
- Pattern explosion test.
- Reconnect storm test.
- Slow-subscriber fanout test.

Red flag:
- If a single slow subscriber poisons fanout or wildcard matching gets superlinear, notice will not feel production-safe.

## DOMAIN: RPC

Performance characteristics:
- Current targets to verify: `sustained_dispatch` at 500,000 ops/sec, `concurrent_tracking` at 625,000 ops/sec, `response_streaming` at 588,235 ops/sec, `scaling_64` at 357,143 ops/sec, and `scaling_256` at 142,857 ops/sec.
- Measure p50, p95, and p99 for request acceptance, worker dispatch, response forwarding, and timeout completion.
- Measure memory cost per in-flight RPC and per active reply inbox.
- Measure connection cost per client with unique correlation state.

Scaling cliffs to discover:
- The first visible knee should appear between 64 and 256 concurrent inflight calls; verify where the sink mutex and reply inbox start queueing.
- Timeout sweeps should show a cliff when many leases expire at once.
- Streaming responses should degrade earlier than single replies because of reassembly and ordering.

Contention questions:
- What happens when many clients hit the same route at once?
- What happens when many requests target the same worker pool?
- What happens when many responses arrive while the correlation table is hot?

Workload shapes to benchmark:
- Many small requests.
- Few large requests.
- Burst versus steady load.
- Response streaming.
- Many routes versus one hot route.

Failure mode questions:
- What happens if a worker disconnects after requests were accepted?
- What happens if a client abandons inflight calls?
- What happens if a timeout storm and a reconnect storm happen at the same time?

Resource limit questions:
- What is the max inflight RPC count before queueing becomes visible?
- What is the max worker pool size before scheduling overhead dominates?
- What is the max pending correlation state before the sink mutex becomes a wall?

Client behavior questions:
- Do clients reuse correlation IDs or prebuilt request frames incorrectly?
- Do abandoned calls leak pending state until timeout?
- Do retry storms create duplicate correlation pressure?

Backpressure questions:
- Does Fitz reject or queue requests before the worker pool is overloaded?
- What signal does a caller receive when a request times out, the worker disconnects, or the correlation ID is unknown?
- Is there a clear distinction between accepted, queued, timed out, and worker-disconnected responses?

Latency sensitivity:
- Strongly lock bound and network bound.
- Tail latency comes from correlation state contention, reply inbox buffering, and transport hops.
- RPC is the most sensitive domain to scheduler noise under contention.

Realistic expectations to validate:
- Starting operational target: 142,857 to 625,000 ops/sec aggregate on the current baseline, depending on whether the test stresses scaling, response forwarding, or dispatch.
- Rough 16-vCPU normalized equivalent: about 8,929 to 39,063 ops/sec/core if the path scales linearly.
- External comparison target: RPC should preserve tail latency under multiclient load, not only hit a good average.

Benchmarks required:
- RPC storm test.
- Worker pool scaling test at 64 and 256 workers.
- Response-streaming test.
- Timeout-sweep stress test.
- Disconnected-worker storm test.

Red flag:
- If p99 climbs with concurrency or correlation correctness is fragile, developers will not trust RPC.

## DOMAIN: LEASE

Performance characteristics:
- Current targets to verify: `mixed_operations_high_load` at 7,692,308 ops/sec and `single_route_intensive` at 5,555,556 ops/sec on the 16-vCPU baseline.
- Measure p50, p95, and p99 for acquire, extend, release, query, and renewal.
- Measure memory cost per active lease and per waiting acquirer.
- Measure connection cost per client that repeatedly renews or retries.

Scaling cliffs to discover:
- The waiting queue per lease should show a hard knee at the configured queue depth; verify the exact failure point.
- Expiry scanning should show visible cost once the number of live leases grows large.
- Renewals on one lease should show the fairness limit where late clients stop making progress.

Contention questions:
- What happens when many clients race for the same lease key?
- What happens when many leases expire at the same tick?
- What happens when one route family carries most of the acquisition traffic?

Workload shapes to benchmark:
- Acquire, extend, release, and query.
- Renewal storms.
- Lease churn.
- Held-then-released leases.
- Same-key contention versus spread-out keys.

Failure mode questions:
- What happens when a lease holder disconnects without releasing?
- What happens when a renewal arrives after expiry?
- What happens if the expiry scanner falls behind and then catches up?

Resource limit questions:
- What is the max lease count the actor can manage comfortably?
- What is the max waiter depth per lease before QueueFull becomes common?
- What is the max wait time clients should be allowed before the system should refuse the request?

Client behavior questions:
- Do clients spam acquire on held leases?
- Do they retry too aggressively after QueueFull or HeldByOther?
- Do disconnect/reconnect loops leave stale waiters behind?

Backpressure questions:
- Does Fitz return HeldByOther, QueueFull, or Timeout quickly enough to prevent retry storms?
- Is there a clear signal for waiting versus failed acquisition?
- Does the lease queue protect the system or merely delay failure?

Latency sensitivity:
- Mostly actor and timer bound.
- Tail latency appears when the lease scan has to catch up or the waiter queue saturates.
- CPU is likely the limit before network on local workloads.

Realistic expectations to validate:
- Starting operational target: 5,555,556 to 7,692,308 ops/sec aggregate on the current baseline for the engine-core paths.
- Rough 16-vCPU normalized equivalent: about 347,222 to 480,769 ops/sec/core if the path scales linearly.
- External comparison target: lease behavior must stay fair and deterministic under churn; raw speed is useless if correctness slips.

Benchmarks required:
- Lease renewal storm.
- Same-key acquire contention.
- Queue-depth exhaustion test.
- Expiry-scan catch-up test.
- Disconnect-and-reacquire test.

Red flag:
- If leases expire late, starve, or admit too many waiters, the domain loses its reliability story.

## DOMAIN: SCHEDULE

Performance characteristics:
- Current targets to verify: `create_operation` at 5,556 ops/sec, `cancel_operation` at 833 ops/sec, `list_10` at 3,333,333 ops/sec, `list_100` at 5,000,000 ops/sec, `list_1000` at 6,250,000 ops/sec, and `mixed_workload` at 1,538 ops/sec.
- Measure p50, p95, and p99 for create, cancel, list, and due-fire paths.
- Measure memory cost per active schedule and per cached cron expression.
- Measure connection cost per client that creates or cancels schedules frequently.

Scaling cliffs to discover:
- Due-fire scanning should show a knee when many schedules become ready in the same tick.
- Cancel-heavy workloads should show the point where stale heap entries create avoidable scan work.
- Cron parsing should show the break between cache hit and cache miss behavior.

Contention questions:
- What happens when many clients create schedules on the same realm or route family?
- What happens when many schedules fire at the same time?
- What happens when create and cancel churn overlaps with due scans?

Workload shapes to benchmark:
- Create/delete bursts.
- Recurring schedule churn.
- Overlapping cron patterns.
- Due-at-the-same-time bursts.
- List 10, 100, and 1000 schedules.

Failure mode questions:
- What happens if the process restarts while many schedules are due?
- What happens if a schedule is canceled right before its fire time?
- What happens if the due scan falls behind and then catches up?

Resource limit questions:
- What is the max active schedule count before memory or scan cost becomes unacceptable?
- What is the max unique cron count before the cache grows too large?
- What is the max due count per tick before trigger latency becomes visible?

Client behavior questions:
- Do clients repeatedly create and cancel the same schedule?
- Do they list after every mutation and force cache invalidation?
- Do they create many unique cron strings and blow out the cache?

Backpressure questions:
- Does Fitz throttle schedule creation when scan lag grows?
- Does it return a clear busy or queue-full signal on overload?
- Is trigger accuracy protected even when the system is saturated?

Latency sensitivity:
- CPU bound on cron parsing, heap operations, and due scanning.
- Storage bound on create/cancel persistence.
- Tail latency appears when many schedules become due at once or churn invalidates caches.

Realistic expectations to validate:
- Starting operational target: 833 to 6,250,000 ops/sec aggregate on the current baseline, depending on whether the path is cancel, create, or list.
- Rough 16-vCPU normalized equivalent: about 52 to 390,625 ops/sec/core if the path scales linearly.
- External comparison target: schedule accuracy matters more than raw create speed; missed or early triggers are the real blocker.

Benchmarks required:
- Schedule flood test.
- Cancel churn test.
- Overlapping cron storm.
- Due-fire catch-up test.
- List-cache invalidation test.

Red flag:
- If triggers slip, duplicate, or get lost under load, scheduling will not be production credible.

## Benchmark Matrix to Build Next

- KV: hot partition test, prefix scan growth test, large transaction sweep, transaction retry storm.
- Stream: append storm, replay catch-up stress, large payload append sweep, multi-writer conflict test.
- Queue: worker scaling test, slow consumer test, redelivery storm, cold start recovery test.
- Notice: broadcast storm, subscriber churn test, pattern explosion test, slow-subscriber fanout test.
- RPC: request storm, worker pool scaling test, timeout-sweep test, disconnected-worker storm.
- Lease: renewal storm, queue-depth exhaustion test, expiry-scan catch-up test, disconnect-and-reacquire test.
- Schedule: schedule flood test, cancel churn test, overlapping cron storm, due-fire catch-up test.

## How To Use This Checklist

- Treat every bullet above as a benchmark or validation question, not as a theoretical claim.
- Record p50, p95, p99, memory, and error behavior for every scenario before calling a domain production-ready.
- If a domain fails the red flag condition, stop optimizing throughput and fix the failure mode first.