---
name: askr-api-integration
description: Use when integrating Askr apps with generated API clients, transport adapters, auth headers, cancellation, DTO mapping, error normalization, retries, and event-sourced consistency metadata.
---

# Askr API Integration

Use this for the boundary between Askr feature code and backend APIs. The goal is one adapter boundary, deterministic DTO mapping, and preserved cancellation and consistency metadata.

## Use This When

- Frontend code crosses an HTTP, SSE, WebSocket, or generated-client boundary.
- DTOs need mapping before they enter features or UI.
- Auth headers, retries, request IDs, or error normalization belong in one place.
- Event-sourced writes need version, cursor, or event metadata for reconciliation.

## Inspect First

- Existing `src/adapters` generated clients and transport wrappers
- Existing `src/features` query or mutation workflows
- Existing `src/shared` config, error formatting, and auth helpers
- API DTO naming, version fields, event IDs, cursor fields, and request IDs

## Choose The Boundary

- `src/adapters`: generated clients, raw transport, auth header injection, and low-level retry behavior.
- `src/features/<feature>`: app-level queries, mutations, DTO-to-model mapping, and workflow state.
- `src/shared`: cross-cutting config, error normalization, formatters, and request tracing.
- Components should consume app models and feature state, never raw transport DTOs.

## Do This In Order

1. Keep raw client calls in the adapter boundary.
2. Forward `AbortSignal` through every cancellable layer.
3. Map DTOs into app models before they leave the feature or adapter boundary.
4. Normalize errors into user-safe messages plus machine-readable diagnostics.
5. Preserve request IDs, versions, cursors, or event IDs when the UI needs freshness or reconciliation truth.
6. Retry only idempotent reads or commands with explicit idempotency keys.

## Copy This Shape

```ts
export async function listAccounts({ signal }: { signal: AbortSignal }) {
  const response = await accountsClient.list({ signal });

  return {
    items: response.items.map(toAccount),
    version: response.version,
    lastEventId: response.last_event_id,
  };
}
```

## Never Do These

- Returning raw API DTOs directly to page props.
- Hiding `signal`, request IDs, or version metadata because the first screen does not need them yet.
- Retrying a non-idempotent command from the UI without a server-recognized idempotency key.
- Putting raw clients in `src/pages` or generic UI components.

## Validate

- Every async adapter accepts and forwards `signal`.
- Errors normalize into user-safe messages and machine-readable codes.
- DTO mapping is deterministic and tested where the boundary matters.
- Event or version metadata reaches query or mutation state when consistency matters.

## Done When

- Components no longer see raw transport DTOs.
- `signal`, request IDs, and consistency metadata survive the adapter boundary.
- Retry behavior is explicit and safe.
- Error normalization is ready for both user copy and machine-readable handling.

## Handoff

- Use `askr-query-mutation` when feature state must invalidate or reconcile shared reads.
- Use `askr-auth-access` when auth headers or session policy shape the adapter behavior.
- Use `askr-observability-debugging` when request IDs, traces, and safe diagnostics must survive end to end.
