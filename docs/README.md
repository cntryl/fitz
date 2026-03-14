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

`storage-invariants -> architecture -> durability -> recovery -> testing`

### Contributing To Runtime Correctness

`architecture -> route-design -> storage-invariants -> recovery -> testing`

### General Usage

`overview -> quick-start -> api-guide -> troubleshooting`

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

- [clients/CLIENT_SPEC.md](clients/CLIENT_SPEC.md)
- [clients/CLIENT_IMPLEMENTATION_GUIDE.md](clients/CLIENT_IMPLEMENTATION_GUIDE.md)
- [clients/CLIENT_ACCEPTANCE_CRITERIA.md](clients/CLIENT_ACCEPTANCE_CRITERIA.md)
- [clients/CONNECTION_FLOW.md](clients/CONNECTION_FLOW.md)
- [admin/ADMIN_API.md](admin/ADMIN_API.md)
