# Sprint 15: Generic Resource Detail Workbench

## Objective

Make non-queue resource detail pages coherent and useful while respecting each domain's semantics.

## Routes and Files

- `/kv/{realm}/{area}/{resource}`
- `/lease/{realm}/{area}/{resource}`
- `/notice/{realm}/{area}/{resource}`
- `/rpc/{realm}/{area}/{resource}`
- `/schedule/{realm}/{area}/{resource}`
- `/stream/{realm}/{area}/{resource}`
- `ui/src/pages/app/resource-detail.tsx`
- `ui/src/features/resource/resource-detail-page.tsx`
- `ui/src/components/shared/resource-workbench.tsx`
- `ui/src/components/shared/domain-sidebar.tsx`
- `ui/src/styles/resource.css`

## Tasks

1. Resource scope and header
   Requirements:
   - Header consistently identifies domain, realm, area, and resource.
   - Scope values wrap or truncate intentionally without hiding the domain.
   - Header copy gives the user enough context to understand what resource is being inspected.

   Acceptance Criteria:
   - Users can confirm scope without decoding route params.
   - Long realm, area, and resource values do not break desktop or mobile layout.
   - Header remains consistent across all six generic detail routes.

2. Domain-specific summary semantics
   Requirements:
   - Workbench summary shows the domain's most useful detail metrics first.
   - KV detail labels active transactions and diagnostics as broker-local/session-scoped where applicable.
   - Lease detail presents ownership/waiter state as ephemeral coordination.
   - Notice detail presents subscriptions as live session fanout, not durable history.
   - RPC detail presents workers and pending requests as live request/response state.
   - Schedule detail separates durable timing intent from live delivery/subscription state.
   - Stream detail separates durable offset/watermark/size metadata from live append sessions.

   Acceptance Criteria:
   - Users can tell which parts are durable and which are live/ephemeral.
   - Domain copy feels specific without creating six unrelated page layouts.
   - Raw JSON never dominates the first viewport.

3. Related tables and timeline
   Requirements:
   - Related tables appear only when they add operational value.
   - Timeline states whether events are derived and what limit/scope applies.
   - Timeline entries remain readable on mobile.

   Acceptance Criteria:
   - Empty related tables are not shown as decorative filler.
   - Timeline scope and derived status are visible where timeline data appears.
   - Long IDs and labels do not force horizontal page overflow.

4. Raw payload and comparison state
   Requirements:
   - Raw payload is available but visually secondary.
   - Comparison summary is clear when comparison query params are present.
   - Comparison state integrates with the page without pushing primary detail too low.

   Acceptance Criteria:
   - Users can inspect raw data after understanding the summarized state.
   - Comparison state identifies current and compared scopes.
   - Raw payload and comparison panels remain legible in dark mode.

5. Generic detail coverage
   Requirements:
   - Review all six generic detail routes at desktop and mobile widths.
   - Review loading, refreshing, empty, and error states where supported by fixtures or mocks.
   - Update smoke/query coverage if visible route priorities change.

   Acceptance Criteria:
   - Generic detail pages feel related but not generic in copy.
   - Mobile screenshots show readable scope, summary, timeline, and raw payload access.
   - No page blurs live ephemeral state with durable history/state.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: all six generic resource routes at desktop and mobile.

## Out Of Scope

- Queue resource detail, owned by Sprint 14.
- Resource mutation workflows.
- Durable history features not supported by the API.
