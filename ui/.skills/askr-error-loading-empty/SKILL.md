---
name: askr-error-loading-empty
description: Use when designing Askr loading, empty, error, stale, refreshing, pending-write, retry, disabled, partial-data, and eventual-consistency UX across routes, tables, forms, dashboards, queries, and mutations.
---

# Askr Error Loading Empty

Use this whenever a feature touches async data or failure states. The goal is one truthful UI meaning per async state.

## Use This When

- You added a `resource()`, `createQuery()`, or `createMutation()` path.
- The screen can load, refresh, fail, be empty, or lag behind a write.
- The UX currently hides too much behind one spinner or one toast.
- The user needs truthful state during eventual consistency.

## Inspect First

- The owner of the async state.
- Existing shared alert, empty-state, skeleton, toast, and status components.
- Query fields such as `loading`, `refreshing`, `stale`, and `consistency`.
- Mutation fields such as `pending`, `error`, `result`, and `status`.

## Use This Vocabulary

- Initial loading: no usable data yet.
- Refreshing: old data is visible while new data loads.
- Empty: request succeeded but no records match.
- Error: request failed and user needs recovery or explanation.
- Partial: some data is usable, some failed or is still loading.
- Pending write: command accepted but read model may not reflect it yet.
- Stale: current read model is known or suspected to be behind.

## Do This In Order

1. Render a distinct state for initial load, empty, error, refresh, and pending write.
2. Keep useful old data visible during refresh when it is safe to do so.
3. Use truthful copy such as `refreshing`, `saved, syncing`, or `reconnecting`.
4. Disable only the actions that are actually unsafe.
5. Prefer row-level or local status when only one record is stale or pending.

## Copy This Shape

```tsx
if (accounts.pending && !accounts.value) {
  return <p>Loading accounts...</p>;
}

if (accounts.error && !accounts.value) {
  return <p role="alert">Unable to load accounts.</p>;
}

return (
  <section>
    <Show when={accounts.refreshing || accounts.consistency === "pending-write"}>
      <p role="status">Saved, syncing...</p>
    </Show>

    <Show when={(accounts.value?.items.length ?? 0) === 0}>
      <p>No accounts matched this filter.</p>
    </Show>

    <AccountsTable rows={accounts.value?.items ?? []} />
  </section>
);
```

- One spinner for every async state.
- Empty states that hide errors.
- Toast-only errors for important failed workflows.
- Claiming a write is fully complete before the read side confirms it.
- Clearing useful data during refresh.
- Global blocking UI when only one row or one mutation is actually stale.

## Validate

- Initial, refresh, empty, error, stale, and pending-write states are distinct.
- Retry paths call the real owner.
- Important failures are visible without depending only on color or toast.
- Copy tells the truth about eventual consistency.

## Done When

- The screen no longer hides multiple async truths behind one spinner.

## Handoff

- Use `askr-query-mutation` or `askr-resources-data` when the owning state model is still unclear.
- Use `askr-accessibility` when announcements, focus, or row-level status semantics need review.
