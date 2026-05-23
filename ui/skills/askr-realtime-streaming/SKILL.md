---
name: askr-realtime-streaming
description: Use when building Askr realtime UX with SSE, WebSocket, event streams, reconnects, cursors, cancellation, backpressure-aware UI, optimistic updates, projection lag, and event-sourced state.
---

# Askr Realtime Streaming

Use this for live data, event streams, and projection-driven UI. The goal is one clear stream owner, safe reconnect behavior, and bounded DOM and memory cost.

## Use This When

- You need SSE, WebSocket, or long-lived event streaming.
- The UI depends on reconnect, cursor, or projection-lag behavior.
- A timeline, operator log, or live table needs bounded updates.
- You need to reconcile stream events with queries or feature-owned state.

## Inspect First

- Adapter support for SSE, WebSocket, polling, or long-running requests
- Event schema: event ID, sequence, aggregate ID, type, timestamp, and payload
- Query keys and mutation invalidation affected by streamed events
- Reconnect and resume requirements

## Do This In Order

1. Own the stream lifecycle in a route or feature container, not a leaf row or list item.
2. Preserve `lastEventId` or cursor for reconnect.
3. Apply events idempotently by event ID or sequence.
4. Detect gaps and refetch or invalidate the affected read model instead of guessing.
5. Bound long-running buffers so memory and DOM cost stay predictable.
6. Show `connected`, `reconnecting`, `stale`, or `failed` state when freshness matters.

## Copy This Shape

```ts
const MAX_EVENTS = 200;

function applyEvent(nextEvent: StreamEvent) {
  setEvents((current) => {
    if (current.some((event) => event.id === nextEvent.id)) {
      return current;
    }

    return [...current, nextEvent].slice(-MAX_EVENTS);
  });
  setLastEventId(nextEvent.id);
}
```

## Never Do These

- Assuming streamed events arrive exactly once or in order.
- Clearing the screen during reconnect.
- Unbounded in-memory event lists.
- Mixing transport code into pages or components.
- Treating command success as projection success.

## Validate

- Reconnect resumes from a cursor or falls back to refetch.
- Duplicate and out-of-order events are safe.
- Projection lag has visible UI.
- Stream teardown happens on navigation or unmount.

## Done When

- Stream ownership is clear and not buried in leaf components.
- Reconnect, duplicate, gap, and teardown paths are handled.
- Memory and DOM cost stay bounded for long-running screens.
- Projection success is never inferred from command success alone.

## Handoff

- Use `askr-query-mutation` when stream events reconcile shared queries.
- Use `askr-api-integration` when cursor, event ID, or transport shape is unclear.
- Use `askr-observability-debugging` when reconnect failures or duplicate events need operator diagnostics.
