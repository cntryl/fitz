# Troubleshooting

Use this guide to map symptoms to the canonical labels used by admin diagnostics.

## Canonical Labels

- `healthy`: no active pressure detected.
- `throughput`: work is moving, but the surface is load-bearing and should be watched.
- `contention`: multiple actors are competing for the same resource or route.
- `backlog_growth`: backlog or lag is growing faster than it drains.
- `stale_handoff`: a durable handoff or scheduled action is overdue.
- `dead_letter_pressure`: retries are accumulating in dead letters.
- `worker_starvation`: work is waiting for workers, sessions, or owners.
- `data_loss_risk`: reserved for cases where the control plane can see a durability gap.

## Reading A Snapshot

- `current_stage` is the canonical diagnosis label.
- `severity` tells you how urgently an operator should look.
- `trend` tells you whether the pressure is growing, steady, shrinking, or stalled.
- `explanation_hints` are deterministic fragments that describe the diagnosis and the live-versus-durable context.
- `recommended_next_query` should point to a bounded follow-up, usually a resource events lookup.

## Durable Versus Live State

- Use `ephemeral` for live coordination state that resets on disconnect or restart.
- Use `durable` for state that survives restart and is part of the persisted record.
- Use `mixed` when a diagnosis combines both durable depth and live ownership or process state.

The current troubleshooting surfaces are intentionally conservative:

- queue dead letters are durable failure state with live retry pressure
- schedule overdue handoff is durable ownership state with live lateness
- RPC and lease pressure are mostly live coordination state
- queue backlog and stream lag can be mixed durable/live surfaces

## Domain Matrix

| Domain | Can answer now | Later |
| --- | --- | --- |
| `kv` | Open transactions, oldest idle time, owner session, recent transition history | Conflict depth, churn trends, richer ownership analysis |
| `queue` | Backlog, inflight, delayed, dead letters, oldest age, resource events, compare | Redelivery counts, delay age buckets, dead-letter transition counts, backlog age distributions, complete/reject counters |
| `rpc` | Workers registered, pending requests, oldest pending age, route spread, resource events, compare | Timeout counts, backpressure counts, wrong-correlation counts, late-response drop counts, pending depth by route |
| `lease` | Active leases, oldest lease age, resource events, compare | Waiter depth, invalid-token counts, forced-release counts, ownership churn counts |
| `stream` | Offsets, watermarks, live append sessions, resource events, compare | Lag distributions, append-session churn, replay/read latency indicators, conflict or rejection counters |
| `notice` | Active subscriptions, route fanout, resource events, compare | Unsubscribe churn, delivery failures, route concentration, notification drop counts |
| `schedule` | Enabled state, next run, last run, overdue handoff, resource events, compare | Overdue normalization counts, persistence failure counts, claim retry counts, fire-latency breakdowns |

## How To Use It

1. Start with the label, not the raw counter.
2. Check whether the diagnosis is mostly `ephemeral`, `durable`, or `mixed`.
3. Inspect the resource events endpoint for the bounded transition history.
4. Use compare when the question is "what changed?" rather than "what is broken?"
