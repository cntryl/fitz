---
name: askr-observability-debugging
description: Use when adding Askr observability, debugging, structured logs, request IDs, trace IDs, query and mutation diagnostics, event-sourced replay context, agent-run audit trails, and dev-only diagnostics.
---

# Askr Observability Debugging

Use this when failures must be explainable to users, developers, or operators. The goal is preserved correlation data, safe user-facing errors, and diagnosable stale or event-driven failures.

## Use This When

- A failure needs request, trace, command, run, or event correlation.
- You need safe user-visible error copy plus deeper operator diagnostics.
- The UI depends on stale-state or eventual-consistency diagnostics.
- An agent workflow, realtime flow, or write path needs audit-friendly tracing.

## Inspect First

- Existing error normalization and logging helpers in `src/shared`
- API request and response metadata available from adapters
- Query and mutation consistency or error states
- Agent run IDs, command IDs, event IDs, and audit requirements

## Preserve These IDs

- Request ID or trace ID.
- User, session, or workspace ID where safe.
- Command ID, idempotency key, aggregate ID, version, event ID, or projection cursor.
- Query key and invalidation prefix.
- Run ID, tool call ID, approval ID, and artifact ID for agentic workflows.

## Do This In Order

1. Preserve correlation metadata at the adapter boundary.
2. Normalize errors into a user-safe message plus structured diagnostics.
3. Show safe copy to users and deeper context only in logs or dev-only panels.
4. Preserve stale-state, projection, or replay context when eventual consistency matters.
5. Add focused tests for duplicate-event, stale-state, or correlation behavior when the flow depends on it.

## Copy This Shape

```ts
const normalized = normalizeApiError(error, {
  requestId,
  commandId,
  eventId,
  queryKey: "accounts:list",
});

logError("accounts.refresh.failed", normalized.diagnostics);
return {
  message: "Unable to refresh accounts.",
  diagnostics: normalized.diagnostics,
};
```

## Never Do These

- Swallowing errors in async handlers.
- Console-only diagnostics for production-critical flows.
- Showing raw stack traces or transport payloads to users.
- Losing correlation IDs between adapters, features, and UI errors.
- Logging private prompts, tokens, or sensitive request bodies to preserve debugging context.

## Validate

- Errors include user-safe copy and developer-useful context.
- Eventual-consistency states are diagnosable.
- Agent workflows have audit-friendly run and event IDs.
- Logs do not contain secrets or private prompts.

## Done When

- User-facing failures stay safe and intelligible.
- Operators have enough correlation data to trace the issue.
- Stale or event-sourced failures are diagnosable separately from command success.
- No logs or diagnostics leak secrets or private prompts.

## Handoff

- Use `askr-api-integration` when metadata is being dropped before it reaches the feature layer.
- Use `askr-agent-workflows` when run IDs, tool calls, approvals, or artifacts need user-facing traceability.
- Use `askr-testing-determinism` to prove duplicate-event, stale-state, or error-correlation behavior.
