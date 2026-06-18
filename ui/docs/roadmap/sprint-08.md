# Sprint 08: Notice Overview

## Objective

Make the Notice overview read as live ephemeral fanout, not durable message history.

## Routes and Files

- `/notice`
- `ui/src/pages/app/notice.tsx`
- `ui/src/features/notice/*`
- Shared domain components from Sprint 06.

## Tasks

1. Notice metric priority
   Requirements:
   - Lead with active subscriptions, publish rate, delivery drops, and wildcard limit rejects.
   - Subscription and publish-rate signals are visibly distinct.
   - Delivery drops and wildcard rejects are treated as risk signals when non-zero.

   Acceptance Criteria:
   - The first viewport answers: "Is live fanout healthy right now?"
   - Drops and rejects are easy to spot without scanning secondary detail.
   - Primary metrics fit the shared overview rhythm from Sprint 06.

2. Notice semantic copy
   Requirements:
   - Header, sidebar, and state copy describe Notice as live fanout only.
   - Copy never implies durable Notice replay, storage, or message history.
   - Charts or comparison visuals do not suggest historical persistence.

   Acceptance Criteria:
   - No visible copy uses durable-history language for Notice.
   - Empty state describes no currently visible Notice realms/resources.
   - Live subscription wording is clear in both header and sidebar context.

3. Notice inventory and sidebar
   Requirements:
   - Realm and resource inventory points users toward active subscription scopes.
   - Sidebar adds live fanout context rather than repeating the metric table.
   - Resource labels and subscription scope copy remain readable on mobile.

   Acceptance Criteria:
   - Users can identify the scope of active subscriptions.
   - Inventory rows make useful drill-down paths obvious.
   - Sidebar content does not imply replay or stored delivery.

4. Notice states and screenshots
   Requirements:
   - Review loading, refreshing, empty, and error states for Notice-specific language.
   - Review mobile resource inventory and metric card density.
   - Update page smoke or query tests if visible priorities change.

   Acceptance Criteria:
   - Empty state makes clear there are no currently visible Notice realms/resources.
   - Error state names Notice overview loading failure.
   - Screenshot review covers notice loaded, notice empty, and notice mobile.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: notice loaded, notice empty, notice mobile.

## Out Of Scope

- Subscription management.
- Notice resource detail redesign.
