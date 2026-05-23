---
name: askr-app-builder
description: Use when building or substantially extending an Askr application, choosing scaffolds, planning app architecture, combining routing, state, data, UI primitives, theming, charts, SSR/SSG, or deciding which more specific Askr skill should guide the work.
---

# Askr App Builder

Use this only when a task spans multiple owned workflows and no single narrower skill is enough. This is a planning and dispatch skill, not the default starting point for everyday feature work.

## Use This When

- A new feature spans routing, data ownership, UI composition, and validation together.
- You need to choose a scaffold or template path before implementation.
- You need to break a broad feature into smaller owned slices.
- The task touches multiple branches, features, and render surfaces at once.

## Do Not Use This First When

- The task is only about one route, one component, one async owner, or one UI surface.
- File ownership is unclear but still local; use `askr-project-structure` instead.
- The hard problem is already clearly routing, state, theming, auth, or validation.

## Inspect First

- `AGENTS.md` when it exists
- `package.json`
- `src/main.tsx`
- The top-level route registry and nearest branch layout
- Existing `src/features`, `src/adapters`, `src/shared`, and `src/styles` ownership
- Existing tests for the surfaces involved

## Plan The Slice

1. Pick the template or existing app baseline.
2. Identify the route owner, layout owner, feature owner, adapter owner, and validation owner.
3. Split the work into smaller slices that can each be guided by one narrower skill.
4. Apply those narrower skills in sequence instead of keeping the whole task under this one.
5. End with `askr-testing-determinism` once the slices are implemented.

## Choose The Baseline

- New product app: `askr create startkit <name>`.
- Minimal interactive app: `spa`.
- Server-rendered app: `ssr`.
- Static or docs site: `ssg`.
- Existing app: follow its current route, layout, style, and test conventions before introducing new ones.

## Route The Work To Narrower Skills

- File placement or ownership: `askr-project-structure`.
- Route tree, metadata, shell boundaries, navigation: `askr-routing-layouts`.
- Local state or keyed rendering: `askr-runtime-reactivity`.
- Route-owned async reads: `askr-resources-data`.
- Shared keyed reads and writes: `askr-query-mutation`.
- Async truth states: `askr-error-loading-empty`.
- CRUD workflow: `askr-forms-tables-crud`.
- Auth and access: `askr-auth-access`.
- Theming and UI composition: `askr-theming` and `askr-ui-composition`.
- Realtime or event streams: `askr-realtime-streaming`.
- SSR or SSG: `askr-ssr-ssg`.
- Final validation: `askr-testing-determinism`.

## Copy This Shape

```text
route owner      -> src/pages/app/_routes.tsx
layout owner     -> src/pages/app/_layout.tsx
feature owner    -> src/features/agents/
adapter owner    -> src/adapters/agents-client.ts
validation owner -> tests/ or nearest package test script
```

## Never Do These

- React-shaped defaults such as `useEffect` data loading or implicit mutable state.
- Mixing route registration, data transport, layout shell, and visual theme logic in one component.
- Treating this skill as a substitute for narrower workflow skills.
- Raw interactive HTML when an `@askrjs/ui` primitive owns the behavior.
- App-local layout, shell, card, nav, feedback, or form primitives when `@askrjs/themes` already owns the surface.
- Hardcoded `--ak-*` token literals in runtime TypeScript or JavaScript.

## Validate

- The app has one clear route tree.
- Route branches, layouts, feature workflows, adapters, shared helpers, UI behavior, and theme concerns sit in separate layers.
- Loading, empty, error, disabled, and narrow-screen states are explicit.
- `npm run check` or the closest available project check passes.

## Done When

- The chosen template path still matches the app surface.
- Each slice is routed to one narrower skill.
- Route, layout, feature, adapter, shared, and theme concerns are separated.
- No foreign framework defaults or invented primitives slipped in.

## Handoff

- Use the narrower skill that owns the next slice.
- Use `askr-testing-determinism` before finalizing the assembled result.
