# Sprint 11: Queue Overview

## Objective

Make the Queue overview communicate durable work delivery pressure and route users toward queue resource inspection.

## Routes and Files

- `/queue`
- `ui/src/pages/app/queue.tsx`
- `ui/src/features/queue/queue-query.ts`
- `ui/src/features/queue/queue-mappers.ts`
- Shared domain components from Sprint 06.

## Requirements

- Lead with ready, delayed, inflight, dead-letter, total pending, and oldest backlog age.
- Copy describes Queue as durable work delivery.
- Dead-letter and oldest-age pressure must be visible without digging.
- Resource inventory should make queue scope drill-down obvious.
- Empty state describes no visible queue realms/resources.

## Deliverables

- Queue metric ordering revised for operator priority.
- Header/sidebar copy audited for durable work semantics.
- Empty/error/loading/refreshing states reviewed.
- Mobile screenshot reviewed for backlog/dead-letter readability.

## Acceptance Criteria

- The first viewport answers: "Is work waiting, owned, delayed, or dead-lettered?"
- Dead-letter count is never visually treated as a neutral metric when non-zero.
- Resource drill-down path is obvious.
- Durable delivery language does not imply exactly-once processing.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: queue loaded, queue empty, queue mobile.

## Out Of Scope

- Queue resource detail, owned by Sprint 14.
- Queue mutation workflows.
