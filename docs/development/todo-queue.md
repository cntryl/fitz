# TODO - Queue

## Classification
- Durable domain for committed messages and indexes.
- Inflight delivery state, lease tokens, and some retry coordination remain ephemeral.
- Queue durability should stay bounded and practical; this domain does not need a rewrite.

## Current Reality
- Message bodies and core indexes are Midge-backed.
- Recovery can rebuild from storage, but cold-start rebuilds may still be expensive.
- The domain keeps warm per-queue actors in memory.
- Retry/idempotency semantics are not yet stated as precisely as the code needs.

## Focus
- Keep the durable core intact.
- Make the failure model explicit and honest.
- Stop cold actors from growing memory usage without bound.

## Concrete Tasks
- [ ] Document exactly what survives restart: committed messages yes, inflight leases and tokens no.
- [ ] Align idempotency and retry markers with the failure model actually promised.
- [ ] Add idle eviction for cold per-queue actors in the boot sink.
- [ ] Review comments and docs that overstate synchronous commit semantics when buffered writes are used.
- [ ] Keep recovery paths fast when index metadata is present and well-validated when it is not.

## Non-Goals
- Exactly-once delivery guarantees.
- Durable inflight lease ownership across broker restart.
- A full queue architecture rewrite.

## Verification
- [ ] Prove ack/delete success is only reported after durable commit.
- [ ] Prove restart recovers committed messages and indexes.
- [ ] Prove inflight tokens are treated as ephemeral across reconnect or restart.
- [ ] Prove idle actor eviction does not lose committed queue state.

## Files To Touch First
- `src/domains/queue/actor.rs`
- `src/boot/domains/queue_sink.rs`
- `src/utils/idempotency.rs`
- `tests/queue_advanced.rs`
- `tests/queue_e2e.rs`