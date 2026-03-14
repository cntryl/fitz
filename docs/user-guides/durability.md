# Durability

Durability in Fitz depends on domain behavior and configured storage guarantees.

## Practical Expectations

- In-memory paths prioritize latency over crash survivability.
- Persistent paths depend on storage backend and commit mode.
- Acknowledgment timing must be interpreted together with operation type.

## Read This With

- [transaction-durability-contract.md](transaction-durability-contract.md)
- [development/storage-invariants.md](../development/storage-invariants.md)
- [development/recovery-internals.md](../development/recovery-internals.md)
