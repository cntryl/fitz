# Sprint 07: Lease Overview

## Objective

Make the Lease overview communicate ephemeral ownership coordination clearly and compactly.

## Routes and Files

- `/lease`
- `ui/src/pages/app/lease.tsx`
- `ui/src/features/lease/*`
- Shared domain components from Sprint 06.

## Tasks

1. Lease metric priority
   Requirements:
   - Lead with active leases, waiter depth, oldest lease age, and ownership pressure.
   - Invalid token rejects, forced releases, and acquire timeouts are visible as risk signals.
   - Oldest age uses a readable duration unit and does not look like a timestamp.

   Acceptance Criteria:
   - The first viewport answers: "Are there active owners or waiters?"
   - Risk counters are easy to spot when non-zero.
   - Primary metrics fit the shared overview rhythm from Sprint 06.

2. Lease semantic copy
   Requirements:
   - Header, sidebar, and state copy describe leases as ephemeral ownership coordination.
   - Copy does not imply durable ownership continuity after disconnect or restart.
   - Token and ownership language remains precise and operational.

   Acceptance Criteria:
   - No visible copy promises ownership recovery, replay, or durable continuity.
   - Header and sidebar make the current coordination scope clear.
   - Empty state reads as current broker snapshot state.

3. Lease inventory and sidebar
   Requirements:
   - Realm inventory makes currently visible lease scopes obvious.
   - Sidebar adds diagnostic context instead of repeating the metric table.
   - Drill-down affordances follow the shared domain overview pattern.

   Acceptance Criteria:
   - Users can identify which lease scopes are active or waiting.
   - Sidebar content is useful at desktop width and does not dominate the page.
   - Resource labels do not overflow on mobile.

4. Lease states and screenshots
   Requirements:
   - Review loading, refreshing, empty, and error states for Lease-specific language.
   - Review metric readability at mobile width.
   - Update page smoke or query tests if visible priorities change.

   Acceptance Criteria:
   - Empty state states that no lease realms are visible in the current broker snapshot.
   - Error state names Lease overview loading failure.
   - Screenshot review covers lease loaded, lease empty, and lease mobile.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: lease loaded, lease empty, lease mobile.

## Out Of Scope

- Lease mutation actions.
- Lease resource detail redesign.
