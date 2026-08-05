# Fitz Documentation

Fitz is a single-node application broker with seven primitives: Stream, Queue,
Notice, RPC, KV, Lease, and Schedule. Their persistence and session semantics
are documented separately.

This index is the public reading path. It keeps product usage, operations, client implementation, and contributor internals separate.

## Learn Fitz

- [Overview](user-guides/overview.md)
- [Domain boundaries](development/domain-boundaries-spec.md)
- [Architectural laws](development/architectural-laws.md)
- [Routing model](development/routing-design.md)
- [Durability](user-guides/durability.md)
- [Transaction durability contract](user-guides/transaction-durability-contract.md)

## Build Apps

- [Quick start](user-guides/quick-start.md)
- [API guide](user-guides/api-guide.md)
- [Troubleshooting](user-guides/troubleshooting.md)
- [FAQ](user-guides/faq.md)
- [Environment variables](user-guides/vars.md)
- [Auth0 setup](user-guides/auth0.md)

## Operate Fitz

- [Storage setup](operations/cloud-setup.md)
- [Auth and browser deployment](operations/auth-browser-deployment.md)
- [Probes and observability](operations/observability.md)
- [Operations runbook](operations/operations-runbook.md)
- [Resource limits](operations/resource-limits.md)
- [Performance tuning](operations/performance-tuning.md)
- [Release checklist](operations/release-checklist.md)
- [Migration guide](operations/migration-guide.md)
- [Admin API](admin/admin-api.md)
- [MCP control-plane safety](admin/mcp-control-plane.md)

## Implement Clients

- [Client specification](clients/client-spec.md)
- [Connection flow](clients/connection-flow.md)
- [Client requirements](clients/client-requirements.md)
- [Client acceptance criteria](clients/client-acceptance-criteria.md)
- [Client implementation guide](clients/client-implementation-guide.md)
- [Cross-language conformance runner](clients/cross-language-conformance-runner.md)

## Understand Internals

- [Architecture](development/architecture.md)
- [Architecture modules](development/code-architecture-modules.md)
- [Architecture runtime flows](development/code-architecture-runtime-flows.md)
- [Architecture domain patterns](development/code-architecture-domain-patterns.md)
- [Storage invariants](development/storage-invariants.md)
- [Recovery internals](development/recovery-internals.md)
- [Format compatibility](development/format-compatibility.md)
- [Release policy](development/release-policy.md)
- [Support matrix](development/support-matrix.md)
- [Testing](development/testing.md)
- [Benchmark guidelines](development/benchmarks.md)
- [Stress benchmark contract](development/stress-bench-contract.md)
- [Performance loop](development/perf-loop.md)
- [Benchmark targets](development/bench-targets.md)
