# Summary

Describe the change in one or two paragraphs.

## What Changed

-

## Verification

- [ ] `cargo test --workspace`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo fitz-tools validate-tests --summary`
- [ ] Relevant docs updated when behavior or guarantees changed

## Domain Boundary Review

Complete this section when the PR touches Fitz domain behavior, semantics, routing, persistence, recovery, or cross-domain composition.

- [ ] No domain now claims another domain's primary guarantee
- [ ] Durable versus ephemeral behavior remains explicit
- [ ] Disconnect and restart behavior remains explicit where relevant
- [ ] Cross-domain interactions still name one authoritative domain
- [ ] Any changed guarantees were updated in [docs/development/domain-boundaries-spec.md](docs/development/domain-boundaries-spec.md)
- [ ] Any changed proof details were updated in the relevant file under [docs/todos](docs/todos)

If this PR does not affect domain boundaries, state that explicitly here.

## Notes For Reviewers

- Boundary-sensitive docs: [docs/development/domain-boundaries-spec.md](docs/development/domain-boundaries-spec.md), [docs/todos/todo-all.md](docs/todos/todo-all.md)
- Architecture context: [docs/development/architecture.md](docs/development/architecture.md)
