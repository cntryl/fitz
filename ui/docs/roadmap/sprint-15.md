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

## Requirements

- Scope header is consistent: domain, realm, area, resource.
- Workbench summary shows the domain's most useful detail metrics first.
- Related tables only appear when they add real operational value.
- Timeline states whether events are derived and what limit/scope applies.
- Raw payload is available but visually secondary.
- Comparison summary is clear when comparison query params are present.
- Domain copy does not blur live ephemeral state with durable history/state.

### Per-Domain Requirements

- KV detail: active transactions and diagnostics are labeled as broker-local/session-scoped where applicable.
- Lease detail: ownership/waiter state is presented as ephemeral coordination.
- Notice detail: subscriptions are live session fanout, not durable history.
- RPC detail: workers and pending requests are live request/response state.
- Schedule detail: timing intent is durable, delivery/subscription state is live.
- Stream detail: offset/watermark/size are durable stream metadata; append sessions are live.

## Deliverables

- Resource workbench visual hierarchy refined.
- Related table pattern normalized.
- Timeline and raw payload sections made scannable.
- Comparison state visually integrated without pushing primary detail too low.
- Mobile screenshots for all six generic detail routes reviewed.

## Acceptance Criteria

- Users can tell which parts are durable and which are live/ephemeral.
- Raw JSON never dominates the first viewport.
- Long resource names and IDs wrap or truncate intentionally.
- Timeline entries are readable on mobile.
- Generic detail pages feel related but not generic in copy.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: all six generic resource routes at desktop and mobile.

## Out Of Scope

- Queue resource detail, owned by Sprint 14.
- Resource mutation workflows.
- Durable history features not supported by the API.
