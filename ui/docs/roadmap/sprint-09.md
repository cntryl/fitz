# Sprint 09: RPC Overview

## Objective

Make the RPC overview communicate live request/response health, pending work, and worker availability.

## Routes and Files

- `/rpc`
- `ui/src/pages/app/rpc.tsx`
- `ui/src/features/rpc/*`
- Shared domain components from Sprint 06.

## Requirements

- Lead with pending requests, workers registered, operations/sec, and timeout/failure pressure.
- Copy describes RPC as live request/response, not durable work delivery.
- Worker and pending request signals are visibly paired.
- Realm/resource inventory should guide users toward operations with pending work or missing workers.
- Empty state should say no RPC realms are currently visible.

## Deliverables

- RPC metric ordering revised for operator priority.
- Header/sidebar copy audited for live RPC semantics.
- Empty/error/loading/refreshing states reviewed.
- Mobile screenshot reviewed for table/card density.

## Acceptance Criteria

- The first viewport answers: "Are requests waiting and are workers available?"
- Copy does not imply durable backlog or replay.
- Pending and worker metrics are not visually buried under secondary counters.
- Long operation/resource labels do not overflow.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: rpc loaded, rpc empty, rpc mobile.

## Out Of Scope

- Worker management actions.
- RPC resource detail redesign.
