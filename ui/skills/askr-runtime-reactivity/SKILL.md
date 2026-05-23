---
name: askr-runtime-reactivity
description: Use when writing askr reactive runtime code with state, derive, selector, context, JSX control flow, stable call order, deterministic event handling, or render lifecycle rules.
---

# Askr Runtime Reactivity

Use this when local state, derived values, keyed identity, or runtime determinism matters. The goal is stable helper call order and correct primitive use.

## Use This When

- You are adding local UI state.
- You need a computed value from reactive reads.
- You are rendering keyed lists, selections, or filters.
- You need to keep call order stable across renders.

## Inspect First

- The nearest existing component with similar state or list behavior.
- Existing tests that already cover toggles, selection, or filtering.
- The nearest feature owner if local state may need to become shared later.

## Pick The Primitive

- Local mutable value: `state()` returns a `[getter, setter]` pair.
- Computed value from reactive reads: `derive()`.
- One reactive source fans out to many keyed readers: `selector()`.
- Cross-tree shared value: `defineContext()` and `readContext()`.
- Keyed or identity-sensitive list: `For`.

## Copy This Shape

```tsx
import { derive, selector, state } from "@askrjs/askr";
import { For, Show } from "@askrjs/askr/control";

const [count, setCount] = state(0);
const [selectedId, setSelectedId] = state<number | null>(null);
const doubled = derive(() => count() * 2);
const isSelected = selector(selectedId);
```

`state()` returns a `[getter, setter]` pair. Read with `getter()` and update with `setter(...)`.

## Determinism Rules

- Do not call `state`, `derive`, `selector`, `resource`, query, or mutation helpers conditionally.
- Do not mutate state during render.
- Batch related updates in the event handler that owns the user action.
- Preserve stable keys in `For` for rows and reorderable lists.

## Reject This Shape

```tsx
function BadList({ rows }) {
  if (rows.length > 0) {
    const [selected, setSelected] = state(rows[0].id);
    return rows.map((row, index) => (
      <button key={index} onClick={() => setSelected(row.id)}>
        {selected === row.id ? "Selected" : row.label}
      </button>
    ));
  }

  return null;
}
```

Reject this shape. It creates state conditionally, treats getter/setter values like plain variables, and uses index keys where stable identity matters.

- Reading a getter without calling it.
- React hooks or `useState` in Askr app code.
- Recreating selectors in row components.
- Index keys for data with stable IDs.
- Derived state that performs side effects.

## Validate

- Runtime helpers are top-level and unconditional.
- Getter calls are explicit.
- Lists have stable identity.
- Existing or new tests cover the state change that matters.

## Done When

- No React-style state shape or conditional helper creation slipped in.

## Handoff

- Use `askr-query-mutation` when the next step is shared keyed state.
- Use `askr-resources-data` when the next step is async lifecycle ownership.
- Use `askr-testing-determinism` before closing runtime-sensitive changes.
