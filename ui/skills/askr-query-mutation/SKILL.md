---
name: askr-query-mutation
description: Use when modeling shared server state in Askr with createQuery, createMutation, invalidate, service boundaries, consistency states, pending writes, cache keys, event-sourced read models, and explicit read/write coordination.
---

# Askr Query Mutation

Use this for shared keyed reads and writes that must coordinate across screens or survive beyond one render. The goal is deliberate keys, narrow invalidation, and truthful write reconciliation.

## Use This When

- Multiple screens need the same keyed server state.
- A write must invalidate or reconcile shared reads.
- The UI needs pending, error, stale, or pending-write truth after a mutation.
- Event-sourced or eventually consistent data needs explicit reconciliation.

## Inspect First

- Existing query keys and invalidation prefixes.
- The nearest feature owner for the data.
- The adapter or service boundary that should receive `signal`.
- Existing stale, syncing, or pending-write UI on the same surface.

## Pick The Data Owner First

- One route or container owns the read and no other screen shares it: use `askr-resources-data`.
- Shared keyed read that multiple screens can refresh or invalidate: use `createQuery()`.
- Write with visible pending, error, result, and invalidation behavior: use `createMutation()`.

## Do This In Order

1. Design a stable, prefix-friendly query key.
2. Keep fetch and mutation transport in an adapter or service boundary.
3. Use `createQuery()` for the shared read and `createMutation()` for the write.
4. Invalidate only the affected key or prefix after success.
5. Preserve version, event ID, cursor, or command metadata when the UI must reason about projection lag.

## Copy This Shape

```ts
import { createQuery, invalidate } from "@askrjs/askr/data";

const user = createQuery({
  key: `user:${id}`,
  fetch: ({ signal }) => userService.getUser(id, { signal }),
});

await user.refresh();
invalidate("user:");
```

## Mutation Shape

```ts
import { createMutation } from "@askrjs/askr/data";

const saveUser = createMutation({
  action: (input, { signal }) => userService.updateUser(input, { signal }),
  affects: (input) => [`user:${input.id}`, "users:"],
  afterSuccess: "invalidate",
});

await saveUser.execute({ id, name });
```

- Only use optimistic updates when rollback or refetch behavior is explicit.
- If the backend is event-sourced, prefer truthful `pending-write` or `syncing` UI over pretending the projection already caught up.
- Keep optimistic local intent separate from confirmed read-model state.

## Event-Sourced Consistency

Use this pattern when writes append events and reads come from projections:

- Command success means the write was accepted, not necessarily that every read model is caught up.
- Include command ID, aggregate ID, expected version, observed version, event ID, or projection cursor in mutation results when the backend exposes them.
- Use `affects` and `afterSuccess: 'invalidate'` to mark affected queries as `pending-write` and refresh them.
- Keep the old query data visible while `consistency` is `pending-write`, `refreshing`, or `stale`.
- On stream reconnect or cursor gaps, invalidate the affected query prefix instead of replaying uncertain local state.
- Use `isConsistent` to compare returned read data against expected versions or event IDs.
- Use `reconcile` to retry while a projection is behind, with user-visible stale/syncing feedback.

```ts
const account = createQuery({
  key: `account:${id}`,
  fetch: ({ signal }) => accountsService.getAccount(id, { signal }),
  isConsistent: (data) => data.version >= expectedVersion(),
  reconcile: () => true,
});
```

- Fetching directly in many leaf components.
- Hiding writes inside presentational UI.
- Cache keys that cannot be invalidated predictably.
- Generic query clients or global state abstractions unless the app already owns one.
- Optimistic updates without rollback or refetch.
- Treating write acknowledgement as read-model convergence in event-sourced systems.
- Cache keys that mix route-local UI state with shared server-state identity.

## Validate

- Query keys are stable and prefix-friendly.
- `signal` reaches the service boundary.
- Mutation pending and error states are visible outside the button that triggered them.
- Success invalidates only the affected read model.
- Event or version metadata is preserved when the UI needs to reason about catch-up.

## Done When

- The feature did not grow a second cache or state layer.

## Handoff

- Use `askr-api-integration` when DTO mapping or transport details are the real blocker.
- Use `askr-error-loading-empty` when the hard part is presenting stale, syncing, or retry truth.
- Use `askr-realtime-streaming` when queries must reconcile with live events.
