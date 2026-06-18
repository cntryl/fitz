# Sprint 11: Queue Overview

## Objective

Make the Queue overview communicate durable work delivery pressure and route users toward queue resource inspection.

## Routes and Files

- `/queue`
- `ui/src/pages/app/queue.tsx`
- `ui/src/features/queue/queue-query.ts`
- `ui/src/features/queue/queue-mappers.ts`
- Shared domain components from Sprint 06.

## Tasks

1. Queue metric priority
   Requirements:
   - Lead with ready, delayed, inflight, dead-letter, total pending, and oldest backlog age.
   - Dead-letter and oldest-age pressure are visible without digging.
   - Dead-letter count is visually treated as risk when non-zero.

   Acceptance Criteria:
   - The first viewport answers: "Is work waiting, owned, delayed, or dead-lettered?"
   - Dead-letter and backlog-age signals are findable in the primary metric group.
   - Primary metrics fit the shared overview rhythm from Sprint 06.

2. Queue semantic copy
   Requirements:
   - Header, sidebar, and state copy describe Queue as durable work delivery.
   - Copy does not imply exactly-once processing.
   - Inflight ownership and delivery pressure are described precisely.

   Acceptance Criteria:
   - Durable delivery language is accurate and does not promise exactly-once behavior.
   - Empty state describes no visible queue realms/resources.
   - Error state avoids implying recoverable processing history beyond supported APIs.

3. Queue inventory and drill-downs
   Requirements:
   - Resource inventory makes queue scope drill-down obvious.
   - Realm/resource rows point users toward queues with backlog, inflight work, or dead letters.
   - Long realm, area, and resource names remain readable on mobile.

   Acceptance Criteria:
   - Users can identify which queue resource to inspect next.
   - Drill-down path to queue resource detail is visually obvious.
   - Inventory rows do not overflow the page.

4. Queue states and screenshots
   Requirements:
   - Review loading, refreshing, empty, and error states for Queue-specific language.
   - Review backlog/dead-letter readability at mobile width.
   - Update page smoke or query tests if visible priorities change.

   Acceptance Criteria:
   - Empty and error states are route-specific and visually quiet.
   - Mobile layout keeps ready, delayed, inflight, and dead-letter signals readable.
   - Screenshot review covers queue loaded, queue empty, and queue mobile.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: queue loaded, queue empty, queue mobile.

## Out Of Scope

- Queue resource detail, owned by Sprint 14.
- Queue mutation workflows.
