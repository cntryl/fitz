# Sprint 10: Schedule Overview

## Objective

Make the Schedule overview separate durable timing intent from live handoff/subscription state.

## Routes and Files

- `/schedule`
- `ui/src/pages/app/schedule.tsx`
- `ui/src/features/schedule/*`
- Shared domain components from Sprint 06.

## Requirements

- Lead with active schedules, pending fire claims, executions/minute, and notification/ack failures.
- Copy describes schedule definitions as durable timing intent.
- Live handoff/subscription state must be labeled as current broker state.
- Persistence failure counters are visible and not hidden behind generic "errors".
- Empty state describes no visible schedule realms/resources.

## Deliverables

- Schedule metric ordering revised for operator priority.
- Header/sidebar copy audited for durable-vs-live semantics.
- Empty/error/loading/refreshing states reviewed.
- Mobile screenshot reviewed for dense counters.

## Acceptance Criteria

- The first viewport answers: "Are timing definitions active and are handoffs stuck?"
- Durable timing intent is not confused with durable delivery history.
- Pending fire claims and ack retries are visually findable.
- Failure counters use precise labels.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: schedule loaded, schedule empty, schedule mobile.

## Out Of Scope

- Schedule creation/editing.
- Schedule resource detail redesign.
