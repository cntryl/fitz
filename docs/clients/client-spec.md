# Fitz Client Specification

Stable entrypoint for the Fitz wire protocol and client contract. Detailed sections live in focused subdocs so the reference stays navigable.

## Sections

- [Scope, client model, concurrency, and transports](spec/overview.md)
- [Wire protocol, connection lifecycle, auth, flow control, and routing](spec/wire-routing.md)
- [Route acceptance criteria, global route rules, and route shapes by domain](spec/wire-route-shapes.md)
- [Canonical operation reference and shared client behavior](spec/operations.md)
- [Notice and Stream domain details](spec/notice-stream.md)
- [Queue and RPC domain details](spec/queue-rpc.md)
- [KV domain details](spec/kv.md)
- [Lease domain details](spec/lease.md)
- [Schedule domain details](spec/schedule.md)
- [Constants, TLV registry, acceptance, and broker-specific behavior](spec/registry-acceptance.md)
