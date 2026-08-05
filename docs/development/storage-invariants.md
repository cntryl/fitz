# Storage Invariants

This document defines invariants Fitz relies on for correctness.

## Core Invariants

1. Route-family and realm boundaries are never crossed implicitly.
2. Transaction-scoped writes are applied atomically at the configured commit point.
3. Recovery only replays data that satisfies commit visibility rules.
4. Domain handlers do not perform async waits in the core path.

## Safety Checks

- Validate route parsing before domain operation dispatch.
- Reject malformed payloads deterministically.
- Keep error mapping stable for retry logic.

## Related Docs

- [architecture.md](architecture.md)
- [Routing design](../../routing-design.md)
- [recovery-internals.md](recovery-internals.md)
