---
name: askr-project-structure
description: Use when deciding where Askr app files belong, naming components, separating route-first pages, layouts, features, shared helpers, adapters, reusable UI, tests, and package-owned responsibilities.
---

# Askr Project Structure

Use this before creating, moving, or splitting files. The goal is one obvious owner per file and no parallel architecture.

## Use This When

- You need to add a new file.
- You are tempted to put logic in a page because it is nearby.
- The repo already has routes, features, adapters, or shared helpers and you need the right owner.
- You want to avoid duplicate structures for the same concern.

## Inspect First

- The existing route registry and nearest `_layout.tsx` owner.
- The nearest existing feature folder for the same domain.
- The nearest adapter, shared helper, and reusable component folder.
- Existing tests for the same surface.

## Keep The Existing App Shape

- If the repo uses `src/pages/**/_routes.tsx`, keep using it.
- If the repo uses `src/routes/*`, keep using it.
- If the repo uses `src/shared`, keep shared helpers there.
- If the repo uses `src/lib` for shared helpers, keep using it.
- Do not create a second route tree, a second shared layer, or a second feature layout because one file looked easier.

## Pick The Owner

- URL reachability: nearest route registry file.
- Shell chrome or persistent branch UI: matching `_layout.tsx`.
- Domain workflow, queries, mutations, and feature UI: `src/features/<feature>/`.
- Reusable display-only pieces: the repo's shared components folder.
- Cross-cutting helpers such as formatting or parsing: the repo's shared helper folder.
- Generated clients, raw fetch wrappers, and DTO mapping: `src/adapters/`.
- Visual styling and tokens: CSS or theme layer, not runtime logic.

## Copy This Shape

```text
src/pages/app/billing.tsx          # route-owned page shell
src/features/billing/billing-form.tsx
src/features/billing/billing.query.ts
src/adapters/billing-client.ts
src/shared/format-money.ts
```

## Reject These Shapes

- `src/pages/app/billing.tsx` importing a generated API client directly.
- `src/components/shared/billing-table.tsx` owning billing mutations and route redirects.
- `src/shared/billing.tsx` containing JSX and transport code together.
- API clients in components or UI primitives.
- Business logic or transport code in `src/pages`.
- JSX in `src/shared` or `src/adapters`.
- Duplicate shells across pages.
- One component that owns routing, fetching, mutation, and styling.
- Parallel abstractions for a concern Askr already owns.

## Validate

- Another agent could predict where the file belongs.
- No page directly owns raw transport or DTO mapping.
- No JSX leaked into adapters or shared helper files.
- New folders match the existing repo shape instead of creating a second one.

## Done When

- No duplicate structure was introduced for the same concern.

## Handoff

- Use `askr-routing-layouts` when the hard part is URL ownership.
- Use `askr-resources-data` or `askr-query-mutation` when the hard part is async ownership.
- Use `askr-testing-determinism` once ownership is settled and you need validation.
