# Fitz Documentation

Fitz is a layered broker with async transports and a sync runtime core. This documentation is organized so evaluators, contributors, and operators can follow predictable reading paths.

## What To Read Before Trying Fitz

1. [development/storage-invariants.md](development/storage-invariants.md)
2. [development/architecture.md](development/architecture.md)
3. [user-guides/durability.md](user-guides/durability.md)
4. [development/recovery-internals.md](development/recovery-internals.md)
5. [development/testing.md](development/testing.md)

These documents define behavior guarantees, implementation boundaries, and how those guarantees are tested.

For a short durability contract page, read [user-guides/transaction-durability-contract.md](user-guides/transaction-durability-contract.md).

## Documentation Structure

- [user-guides/](user-guides/) for API and operator-facing usage
- [operations/](operations/) for deployment, runbook, and tuning
- [development/](development/) for architecture, invariants, recovery, and release policy
- [benchmarks/](benchmarks/) for benchmark references and future reports
- [clients/](clients/) for client wire protocol and implementation guidance
- [admin/](admin/) for admin API and operator controls

## Recommended Reading Paths

### Evaluating Fitz

1. [development/storage-invariants.md](development/storage-invariants.md)
2. [development/architecture.md](development/architecture.md)
3. [user-guides/durability.md](user-guides/durability.md)
4. [development/recovery-internals.md](development/recovery-internals.md)
5. [development/testing.md](development/testing.md)

### Contributing To Runtime Correctness

1. [development/architecture.md](development/architecture.md)
2. [development/route-design.md](development/route-design.md)
3. [development/storage-invariants.md](development/storage-invariants.md)
4. [development/recovery-internals.md](development/recovery-internals.md)
5. [development/testing.md](development/testing.md)

### General Usage

1. [user-guides/overview.md](user-guides/overview.md)
2. [user-guides/quick-start.md](user-guides/quick-start.md)
3. [user-guides/api-guide.md](user-guides/api-guide.md)
4. [user-guides/troubleshooting.md](user-guides/troubleshooting.md)

## Important Positioning

- Experimental: yes
- Durability-tested: yes
- Safe enough for careful evaluation: yes
- Production-ready: not yet

See [development/stability-policy.md](development/stability-policy.md) for pre-1.0 compatibility boundaries.

## What To Read Before Calling It Production-Ready

1. [development/one-dot-zero-contract.md](development/one-dot-zero-contract.md)
2. [development/one-dot-zero-readiness-scorecard.md](development/one-dot-zero-readiness-scorecard.md)
3. [development/support-matrix.md](development/support-matrix.md)
4. [development/format-compatibility.md](development/format-compatibility.md)
5. [development/release-policy.md](development/release-policy.md)
6. [operations/production-runbook.md](operations/production-runbook.md)
7. [operations/release-checklist.md](operations/release-checklist.md)

## Fitz-Specific Companion Docs

- [clients/client-spec.md](clients/client-spec.md)
- [clients/client-implementation-guide.md](clients/client-implementation-guide.md)
- [clients/client-acceptance-criteria.md](clients/client-acceptance-criteria.md)
- [clients/connection-flow.md](clients/connection-flow.md)
- [admin/admin-api.md](admin/admin-api.md)
