# Fitz domain guarantees

These summaries describe current Fitz domain semantics. MCP reports current
broker read-model snapshots and does not create durability, replay, or delivery
guarantees beyond the owning domain.

| Domain | Meaning |
| --- | --- |
| Notice | Live, ephemeral fanout. |
| Stream | Durable history and replay. |
| KV | Current authoritative state. |
| Queue | Durable work delivery. |
| RPC | Live request and response. |
| Lease | Ephemeral ownership coordination. |
| Schedule | Durable timing intent. |

`realm` is an application-defined namespace label used by routes and permissions.
`route_family` is a broker routing and isolation key used for assignment and
delivery partitioning. They are independent. An unknown realm remains unknown;
it is never filled from a route family.

Session loss ends the session. It does not preserve ownership continuity.
Cancellation of RPC is cooperative and best effort. A canceled or timed-out
dispatched operation may already have produced side effects and is not safe to
retry automatically.
