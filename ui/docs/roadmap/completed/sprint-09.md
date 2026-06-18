# Sprint 09: RPC Overview

## Objective

Make the RPC overview communicate live request/response health, pending work, and worker availability.

## Routes and Files

- `/rpc`
- `ui/src/pages/app/rpc.tsx`
- `ui/src/features/rpc/*`
- Shared domain components from Sprint 06.

## Tasks

1. RPC metric priority
   Requirements:
   - Lead with pending requests, workers registered, operations/sec, and timeout/failure pressure.
   - Worker and pending request signals are visually paired.
   - Timeout and failure counters are visible as live pressure indicators.

   Acceptance Criteria:
   - The first viewport answers: "Are requests waiting and are workers available?"
   - Pending and worker metrics are not buried under secondary counters.
   - Primary metrics fit the shared overview rhythm from Sprint 06.

2. RPC semantic copy
   Requirements:
   - Header, sidebar, and state copy describe RPC as live request/response.
   - Copy does not imply durable backlog, replay, or work delivery.
   - Worker availability and pending request wording remain live-state scoped.

   Acceptance Criteria:
   - No visible copy uses queue or durable-delivery language for RPC.
   - Empty state says no RPC realms are currently visible.
   - Error state avoids implying recoverable request history.

3. RPC inventory and sidebar
   Requirements:
   - Realm and resource inventory guides users toward operations with pending work or missing workers.
   - Sidebar adds worker/operation context instead of repeating primary metrics.
   - Long operation and resource labels wrap or truncate intentionally.

   Acceptance Criteria:
   - Users can identify operations with pressure or missing capacity.
   - Long operation/resource labels do not overflow.
   - Sidebar context remains useful without dominating the page.

4. RPC states and screenshots
   Requirements:
   - Review loading, refreshing, empty, and error states for RPC-specific language.
   - Review table/card density at mobile width.
   - Update page smoke or query tests if visible priorities change.

   Acceptance Criteria:
   - Empty and error states are route-specific and visually quiet.
   - Mobile layout preserves pending and worker signals.
   - Screenshot review covers rpc loaded, rpc empty, and rpc mobile.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: rpc loaded, rpc empty, rpc mobile.

## Out Of Scope

- Worker management actions.
- RPC resource detail redesign.
