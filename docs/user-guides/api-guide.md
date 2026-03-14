# API Guide

This guide maps user intent to Fitz domain APIs.

## Domain Surfaces

- KV: transactional and non-transactional key operations
- Queue: enqueue, reserve, extend, complete
- Notice: publish and subscribe patterns
- RPC: request/response worker patterns
- Stream: append and consume progression
- Lease: acquire, extend, release
- Schedule: delayed and recurring execution

## Route And Dispatch Model

Routes describe logical target paths while message types determine domain dispatch. See [development/route-design.md](../development/route-design.md).

## Protocol References

- [clients/CLIENT_SPEC.md](../clients/CLIENT_SPEC.md)
- [clients/CONNECTION_FLOW.md](../clients/CONNECTION_FLOW.md)
- [admin/ADMIN_API.md](../admin/ADMIN_API.md)
