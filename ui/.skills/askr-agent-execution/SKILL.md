---
name: askr-agent-execution
description: Use when an AI coding agent needs the canonical execution playbook for reading an Askr repo, choosing the right skill, validating work, and avoiding invented primitives or foreign framework defaults.
---

# Askr Agent Execution

Use this before any edit in an Askr repo. This is the execution loop that every other Askr skill assumes.

## Use This When

- Starting a new task.
- The right Askr skill is not obvious yet.
- You need a fixed read order before editing.
- You need objective done criteria.

## Read In This Order

1. `package.json`
2. `AGENTS.md` when it exists
3. `src/main.tsx`
4. `src/router.tsx` or `src/pages/_routes.tsx`
5. The nearest branch `_routes.tsx` and `_layout.tsx`
6. The nearest `src/features/<feature>/` or `src/components/<feature>/` owner
7. Existing tests for the touched surface
8. `src/styles.css` and `src/styles/*` only when visual behavior changes

## Choose The Next Skill

- File ownership unclear: use `askr-project-structure`.
- Route tree, shell, metadata, or navigation change: use `askr-routing-layouts`.
- Local state, derived values, keyed rendering, or call-order safety: use `askr-runtime-reactivity`.
- One route or container owns an async read: use `askr-resources-data`.
- Shared keyed reads or writes must coordinate across screens: use `askr-query-mutation`.
- Loading, empty, stale, retry, or pending-write truth is the hard part: use `askr-error-loading-empty`.
- You are closing the task and need the right checks: use `askr-testing-determinism`.
- The task spans many app surfaces and no narrower owner is enough: use `askr-app-builder` last, not first.

## Execution Loop

1. Inspect the existing owner files before proposing a new abstraction.
2. Pick one owning skill for the current slice.
3. Make the smallest edit that matches the repo's current structure.
4. Run the narrowest executable validation for that slice.
5. Only widen scope after that validation passes or clearly changes your understanding.
6. Stop when the change is structurally correct, validated, and easy for the next agent to follow.

## Non-Negotiables

- Keep pages route-focused, features workflow-focused, and adapters transport-focused.
- Add files only where another agent could predict they belong.
- Reuse existing layouts, route groups, features, and shared helpers before creating new ones.
- Use `@askrjs/ui` and `@askrjs/themes` before inventing app-local primitives.

## Never Do These By Default

- React hooks or React Router patterns.
- TanStack Query or a generic query-client abstraction.
- Custom routing systems or page-local route state copies.
- App-local `Button`, `Card`, `Panel`, `Sidebar`, `Navbar`, `EmptyState`, `Page`, `Toolbar`, `HStack`, or `VStack` clones.
- Generic store or service-locator layers when `state()`, `resource()`, `createQuery()`, or `createMutation()` already fit.
- Diff-only validation when a narrower executable check exists.

## Validate In This Order

1. Run the narrowest relevant test for the touched behavior.
2. Run the nearest type check or project check.
3. Run build or template-specific checks if the change affects app boot, routing, SSR/SSG, or packaging.
4. Confirm loading, error, empty, stale, pending, focus, and overflow states when user-visible.

Use existing scripts such as `npm test`, `npm run type-check`, `npm run build`, `npm run lint`, or `npm run check` when they exist. Do not invent new validation commands when the repo already defines the contract.

## Done When

- You can name the owner file for every new change.
- The chosen skill matches the slice you edited.
- No foreign framework defaults or parallel architecture were introduced.
- At least one executable validation ran, or the blocker is explicit.
- The next likely skill for follow-up work is obvious.

## Handoff

- Use `askr-project-structure` when the next step is mostly about file placement.
- Use `askr-routing-layouts` when the next step is mostly about URL ownership or shell boundaries.
- Use `askr-testing-determinism` before closing any user-visible or stateful change.
