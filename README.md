# fitz

A high-performance, multi-scheme message broker built on a synchronous actor runtime with a clean durability boundary (Midge for durable domains, memory for ephemeral domains).

Status: early prototype. For the v2 architecture, see the specs under [wip/](wip/).

## quick start

- Requirements: Rust stable, cargo
- Run tests: `cargo test`
- Lint: `cargo fmt --all -- --check` and `cargo clippy -D warnings`

## architecture

- High-level architecture: [wip/ARCHITECTURE.md](wip/ARCHITECTURE.md)
- Domain model (stream/queue/kv/notice/rpc/lease): [wip/DOMAIN_MODEL.md](wip/DOMAIN_MODEL.md)
- Routing hierarchy (Route Family vs realm): [wip/ROUTING_ARCHITECTURE.md](wip/ROUTING_ARCHITECTURE.md)
- Implementation plan: [wip/ROADMAP.md](wip/ROADMAP.md)

## legacy docs

- Roadmap: `todo.md`
- Design: `docs/design_doc.md`
- Specs: `docs/notice_spec.md`, `docs/stream_spec.md`, `docs/queue_spec.md`, `docs/rpc_spec.md`

