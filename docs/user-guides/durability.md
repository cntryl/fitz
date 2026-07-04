# Durability

Durability in Fitz depends on domain behavior, storage configuration, and commit semantics.

## Domain Expectations

| Domain | Durable behavior |
| --- | --- |
| Notice | none; live fanout only |
| Stream | committed history survives according to write mode |
| KV | committed current state survives according to write mode |
| Queue | backlog survives according to queue write policy |
| RPC | none; workers and pending calls are live only |
| Lease | none; ownership is live only |
| Schedule | definitions and pending fire claims survive restart |

## Practical Expectations

- In-memory storage does not provide crash survivability.
- Local and blob/object-backed storage depend on backend characteristics and selected write policy.
- Interpret acknowledgment timing in the context of the domain and operation.
- Queue `fast` mode can lose accepted recent mutations before the background flush window closes.
- Stream and KV durability apply to committed data only; open sessions or transactions do not recover.
- Notice, RPC, and Lease are deliberately ephemeral.

## Read This With

- [transaction-durability-contract.md](transaction-durability-contract.md)
- [development/storage-invariants.md](../development/storage-invariants.md)
- [development/recovery-internals.md](../development/recovery-internals.md)
