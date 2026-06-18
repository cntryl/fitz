# Sprint 10: Schedule Overview

## Objective

Make the Schedule overview separate durable timing intent from live handoff/subscription state.

## Routes and Files

- `/schedule`
- `ui/src/pages/app/schedule.tsx`
- `ui/src/features/schedule/*`
- Shared domain components from Sprint 06.

## Tasks

1. Schedule metric priority
   Requirements:
   - Lead with active schedules, pending fire claims, executions/minute, and notification/ack failures.
   - Pending fire claims and ack retries are visually findable.
   - Persistence failure counters are visible and not hidden behind generic error labels.

   Acceptance Criteria:
   - The first viewport answers: "Are timing definitions active and are handoffs stuck?"
   - Failure counters use precise labels.
   - Primary metrics fit the shared overview rhythm from Sprint 06.

2. Schedule semantic copy
   Requirements:
   - Header, sidebar, and state copy describe schedule definitions as durable timing intent.
   - Live handoff/subscription state is labeled as current broker state.
   - Copy does not confuse durable timing intent with durable delivery history.

   Acceptance Criteria:
   - Durable-vs-live language is clear in header and sidebar context.
   - Empty state describes no visible schedule realms/resources.
   - No visible copy implies exactly-once or historical delivery guarantees.

3. Schedule inventory and sidebar
   Requirements:
   - Realm and resource inventory points toward active schedules and stuck handoffs.
   - Sidebar adds timing/handoff context instead of repeating the metric table.
   - Dense counters remain readable on mobile.

   Acceptance Criteria:
   - Users can find schedules or resources with pending claims.
   - Sidebar context separates definitions from live handoff state.
   - Resource labels and counters do not overflow.

4. Schedule states and screenshots
   Requirements:
   - Review loading, refreshing, empty, and error states for Schedule-specific language.
   - Review dense counter readability at mobile width.
   - Update page smoke or query tests if visible priorities change.

   Acceptance Criteria:
   - Empty and error states are route-specific and do not imply stored delivery history.
   - Mobile layout keeps active, pending, and failure signals readable.
   - Screenshot review covers schedule loaded, schedule empty, and schedule mobile.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: schedule loaded, schedule empty, schedule mobile.

## Out Of Scope

- Schedule creation/editing.
- Schedule resource detail redesign.
