# Sprint 03: Dashboard

## Objective

Turn the dashboard into a first-stop operations overview that helps users decide where to look next.

## Routes and Files

- `/`
- `/admin`
- `ui/src/pages/app/home.tsx`
- `ui/src/features/topology/*`
- `ui/src/components/shared/dashboard-domain-signals.tsx`
- `ui/src/styles/dashboard.css`
- `ui/src/styles/topology-flow.css`

## Requirements

- First viewport communicates broker status, incident posture, and next action.
- The topology flow explains live domain relationships without visual clutter.
- Domain signal summaries are comparable and dense.
- Empty broker state still looks purposeful.
- Refreshing state preserves prior content and clearly marks freshness.
- Error state tells the user what failed and what can still be inspected.

## Deliverables

- Dashboard content hierarchy revised around status, flow, and drill-downs.
- Topology visual density tuned for desktop and mobile.
- Domain cards or panels normalized to one visual pattern.
- Empty, loading, refreshing, and error states reviewed.
- Dashboard route smoke test updated for user-visible priorities.

## Acceptance Criteria

- At `1440px`, users can see broker status, the primary flow area, and at least one drill-down prompt in the first viewport.
- At `390px`, the page reads in a logical order without horizontal scrolling.
- Topology cards do not look like unrelated decorations.
- Status and severity language matches Fitz semantics.
- The dashboard does not duplicate all metrics; it points to domain and metrics pages for detail.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx`
- Screenshot: dashboard desktop, dashboard tablet, dashboard mobile, dashboard refreshing state.

## Out Of Scope

- New broker topology APIs.
- Animated topology behavior.
- Deep metric-family browsing.
