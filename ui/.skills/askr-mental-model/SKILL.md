---
name: askr-mental-model
description: Use when an AI agent or developer needs the canonical Askr mental model, including what Askr is, what it is not, how core primitives fit together, and which React-style assumptions must be rejected.
---

# Askr Mental Model

Use this before building or reviewing Askr code when primitive choice is unclear. This skill exists to keep agents on the native Askr path and off React-shaped defaults.

## Use This When

- Primitive choice is unclear.
- The task looks like React, hooks, or a generic SPA by default.
- You need to decide who owns local state, async reads, or writes.
- You need one obvious Askr-native shape before editing.

## Inspect First

- `AGENTS.md` when it exists
- `src/main.tsx`
- The nearest route file and layout file
- The nearest existing feature that solves a similar problem
- Existing tests for the touched surface

## What Askr Expects

- Route-first ownership.
- Synchronous route components.
- Explicit runtime primitives for state, derived state, lifecycle-owned async work, shared server state, and writes.
- Honest UI for loading, empty, error, stale, and pending-write states.

## What Askr Is Not

- Not a React hooks runtime.
- Not a place to default to `useEffect` data loading.
- Not a reason to add React Router, TanStack Query, or a global state library first.
- Not a place to clone solved `Button`, `Card`, `Sidebar`, `EmptyState`, or shell primitives before checking the existing theme and UI layers.

## Pick The Primitive

- Local mutable UI state: use `state()` as a `[getter, setter]` pair.
- Computed value from reactive reads: use `derive()`.
- One source fans out to many keyed readers: use `selector()`.
- One route or container owns an async read lifecycle: use `resource()`.
- Shared keyed server state used across screens: use `createQuery()`.
- Writes with pending, error, and reconciliation state: use `createMutation()`.
- Keyed or dynamic list identity matters: use `For`.

## Copy This Shape

```tsx
import { derive, selector, state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";

const [selectedId, setSelectedId] = state<string | null>(null);
const isSelected = selector(selectedId);
const items = derive(() => [
  { id: "queued", label: "Queued" },
  { id: "running", label: "Running" },
  { id: "done", label: "Done" },
]);

export default function RunFilterBar() {
  return (
    <nav aria-label="Run status filters">
      <For each={items()} by={(item) => item.id}>
        {(item) => (
          <button
            type="button"
            aria-pressed={isSelected(item.id)}
            onClick={() => setSelectedId(item.id)}
          >
            {item.label}
          </button>
        )}
      </For>
    </nav>
  );
}
```

## Reject These Shapes

- React imports or hook-shaped state as the default solution.
- Effect-driven loading when the UI depends on the async result.
- Treating getters like plain values instead of callable reads.
- Creating runtime helpers conditionally.
- Defaulting to `.map()` for large or dynamic keyed lists where `For` should own identity.
- Hiding data transport, mapping, and mutations inside pages or presentational components.

## Validate

- Primitive choice is explainable in one sentence.
- Route components remain synchronous.
- Keyed UI uses stable IDs.
- No React runtime imports or hook defaults slipped in.
- Async state truth is explicit where the user can observe it.

## Done When

- The code reads like Askr, not like translated React.
- No invented primitives or parallel state layers were added.
- The next narrower skill is obvious from the changed surface.

## Handoff

- Use `askr-routing-layouts` when the next step is route ownership or shell structure.
- Use `askr-resources-data` or `askr-query-mutation` when the next step is async ownership.
- Use `askr-runtime-reactivity` when the next step is local state or keyed rendering behavior.
