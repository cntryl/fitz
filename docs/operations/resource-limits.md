# Resource Limits

Resource limits prevent noisy-neighbor behavior and keep latency predictable.

## Limit Domains

- Connection limits per realm
- Subscription and wildcard pattern limits
- Queue and stream depth limits
- Memory quotas for active sessions and runtime buffers

## Policy Principles

1. Prefer realm-scoped limits over global hard stops.
2. Define soft warning thresholds before hard rejection.
3. Keep limits visible through admin stats and metrics.

## Operational Signals

- Rising rejection counts by realm
- Mailbox depth saturation
- Repeated permission-denied or route-mismatch bursts

## Escalation Actions

1. Apply temporary rate limits to high-impact realms.
2. Scale out compute where bottlenecks are verified.
3. Adjust durable storage throughput where needed.
