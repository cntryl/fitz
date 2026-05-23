---
name: askr-resources-data
description: Use when loading lifecycle-aware async data in Askr with resource, cancellation signals, pending/error/value states, refresh behavior, route/container ownership, consistency-aware refreshes, and async anti-pattern cleanup.
---

# Askr Resources Data

Use this when one route or feature container owns an async read lifecycle. The goal is one clear async owner, explicit cancellation, and truthful refresh behavior.

## Use This When

- One route or feature container owns the read.
- The read should cancel on unmount, navigation, or dependency change.
- Children need plain data props instead of owning their own fetches.
- The data does not need to live as shared keyed state across screens.

## Inspect First

- The nearest route or feature container that already owns async work.
- The adapter or service that should receive `signal`.
- Existing refresh or retry controls for the same surface.
- Existing tests that cover load, retry, or cancellation behavior.

## Use `resource()` Only When

- The read belongs to one owner in one route or feature container.
- The owner should cancel stale requests automatically.
- The screen can keep old data during refresh when safe.

If the same keyed server state must coordinate across screens, stop and use `askr-query-mutation` instead.

## Do This In Order

1. Put the `resource()` call in the smallest route or feature container that owns the data.
2. Pass `signal` into the adapter or fetch layer.
3. Keep route handlers synchronous.
4. Pass `value`, `pending`, `error`, and `refresh()` downward as plain props.
5. Keep stale data visible during refresh unless that would be unsafe.

## Copy This Shape

```tsx
import { resource } from "@askrjs/askr/resources";

function UserCard({ id }: { id: string }) {
  const user = resource(
    async ({ signal }) => {
      const response = await fetch(`/api/users/${id}`, { signal });
      return response.json();
    },
    [id],
  );

  if (user.pending || !user.value) return <p>Loading...</p>;
  if (user.error) return <p role="alert">Unable to load user.</p>;
  return <p>{user.value.name}</p>;
}
```

## Owner Pattern

```tsx
function AccountsPage() {
  const accounts = resource(({ signal }) => loadAccounts({ signal }), []);

  return (
    <AccountsScreen
      accounts={accounts.value}
      pending={accounts.pending}
      error={accounts.error}
      onRefresh={() => accounts.refresh()}
    />
  );
}
```

- Async route components.
- `useEffect`-style data loading.
- Custom cancellation tokens when `signal` exists.
- Hidden loading or error paths.
- Fetching in generic UI primitives.
- Route-local async ownership that should actually be shared keyed state.

## Validate

- The async owner is the smallest container that needs the data.
- `signal` reaches the cancellable work.
- Loading, empty, error, and retry paths are explicit.
- Route handlers stay synchronous.
- Refresh behavior is honest about stale data and projection lag.

## Done When

- The screen did not grow a parallel shared-state layer.

## Handoff

- Use `askr-query-mutation` when this data must become shared keyed state.
- Use `askr-error-loading-empty` when the hard part is truthful state representation.
- Use `askr-testing-determinism` to validate cancellation, retry, and refresh behavior.
