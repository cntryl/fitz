# Durability

Durability in Fitz depends on domain behavior, storage configuration, and commit semantics.

## Practical Expectations

- In-memory paths prioritize latency and do not guarantee crash survivability.
- Persistent paths depend on both storage backend characteristics and selected commit mode.
- Interpret acknowledgment timing in the context of operation type and durability policy.

## Read This With

- [transaction-durability-contract.md](transaction-durability-contract.md)
- [development/storage-invariants.md](../development/storage-invariants.md)
- [development/recovery-internals.md](../development/recovery-internals.md)
