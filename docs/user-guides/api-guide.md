# API Guide

This guide maps common user intent to Fitz domain APIs.

## Domain Surfaces

- KV: transactional and non-transactional key operations
- Queue: enqueue, reserve, extend, complete
- Notice: publish and subscribe patterns
- RPC: request/response worker patterns
- Stream: append and consume progression
- Lease: acquire, extend, release
- Schedule: delayed and recurring execution

## Route and Dispatch Model

Routes describe logical target paths, while message types determine domain dispatch. See the [routing design](../development/routing-design.md).

## Protocol References

- [clients/client-spec.md](../clients/client-spec.md)
- [clients/connection-flow.md](../clients/connection-flow.md)
- [admin/admin-api.md](../admin/admin-api.md)
