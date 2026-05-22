---
name: askr-migration-react
description: Use when translating React-shaped code, habits, or component designs into idiomatic Askr, including replacing useState/useEffect patterns, JSX assumptions, data fetching, routing, context, component APIs, and UI primitive choices.
---

# Askr Migration React

Use this only when a task, sample, or codebase slice is React-shaped and must be translated into idiomatic Askr. It is a transition skill, not a default entrypoint for normal Askr feature work.

## Use This When

- The input code or prompt uses React hooks, React Router, or React-only UI assumptions.
- A generated design mirrors React patterns that need translation before implementation.
- A migration task needs native Askr ownership for state, async work, routing, or primitives.
- The fastest path is to convert a known React shape instead of designing from scratch.

## Inspect First

- The current app's Askr imports and component patterns
- The route tree and feature boundaries already present in the repo
- The React-shaped state, async, routing, and primitive assumptions you are replacing

## Choose The Replacement

- React `useState` -> Askr `state()` as a `[getter, setter]` pair.
- React `useMemo` -> Askr `derive()` when a reactive computation is needed.
- React `useEffect` data loading -> Askr `resource()` or `createQuery()` based on ownership.
- React Router component routes -> Askr route registration with `group()`, `page()`, `route()`, and `fallback()`.
- React context -> Askr `defineContext()` and `readContext()`.
- React keyed list `.map` -> `For` when identity or dynamic list updates matter.

## Do This In Order

1. Identify whether the React-shaped code is really local state, shared data, routing, context, or primitive behavior.
2. Remove React imports and hook assumptions before translating line by line.
3. Replace state, async, routing, and context with the owning Askr primitives.
4. Move feature logic to the correct route, feature, adapter, or shared boundary.
5. Replace React-only UI libraries with Askr-compatible primitives when behavior matters.
6. Validate call order, async truth, and route registration before closing the migration.

## Copy This Shape

```tsx
import { state } from "@askrjs/askr";
import { resource } from "@askrjs/askr/resources";

const [open, setOpen] = state(false);
const user = resource(({ signal }) => loadUser(id, { signal }), [id]);

<button onClick={() => setOpen((value) => !value)}>{open() ? "Close" : "Open"}</button>;
```

## Never Do These

- Importing React hooks or React runtime helpers.
- Treating state getters like values instead of functions.
- Porting React Router route components directly.
- Hiding async work in effects when the UI depends on it.
- Assuming third-party React component packages are compatible.

## Validate

- No React imports remain unless the project intentionally embeds React separately.
- Runtime helper call order is stable.
- Async work has cancellation and visible loading or error states.
- Routing uses Askr route registration.
- Interactive UI uses Askr-compatible primitives.

## Done When

- The translated code reads like Askr instead of line-by-line React porting.
- State, async work, routing, and context use native Askr ownership.
- Feature logic sits in the right file boundaries.
- Remaining UI assumptions are compatible with the app's existing primitives.

## Handoff

- Use `askr-mental-model` when primitive choice is still unclear.
- Use `askr-project-structure` when the migration requires moving files or ownership boundaries.
- Use `askr-query-mutation` or `askr-resources-data` when async ownership is the real blocker.
- Use `askr-testing-determinism` before closing migrated behavior.
