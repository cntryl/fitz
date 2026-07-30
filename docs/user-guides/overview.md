# Fitz Overview

Fitz is a single-node application broker with one route model and seven
primitives.

## The Model

- Notice: live fanout to connected subscribers.
- Stream: durable append and replay of committed history.
- KV: current authoritative state.
- Queue: durable work delivery with reservation, retry, redelivery, and optional dead-letter handling.
- RPC: live request and response dispatch to registered workers.
- Lease: single-broker ownership coordination.
- Schedule: durable timing intent.

## Core Concepts

- Route address: a domain-oriented path such as `stream://realm/area/resource`.
- RouteFamily: broker-internal routing and isolation.
- Realm: opaque application-visible namespace in routes, permissions, and admin payloads.
- Session: authenticated connection context; disconnect destroys session-owned state.

`realm` and `RouteFamily` are separate axes. They are never inferred from each other.

## Guarantees

Fitz is explicit about durability:

- Durable domains recover only committed persisted state.
- Ephemeral domains do not recover live state after disconnect or restart.
- Clients rebuild subscriptions, workers, leases, transactions, and stream resume positions explicitly.

Read [domain-boundaries-spec.md](../development/domain-boundaries-spec.md) for the authoritative domain contract.
