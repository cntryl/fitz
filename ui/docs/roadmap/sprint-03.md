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

## Tasks

1. First-viewport status hierarchy
   Requirements:
   - The first viewport communicates broker status, incident posture, freshness, and the next useful action.
   - Primary status content is dense and operational, not hero or marketing layout.
   - Empty broker state still presents a purposeful dashboard structure.

   Acceptance Criteria:
   - At `1440px`, users can see broker status, the primary flow area, and at least one drill-down prompt.
   - At `390px`, the status hierarchy reads in logical order without horizontal scrolling.
   - Status and severity language matches Fitz semantics.

2. Topology flow clarity
   Requirements:
   - The topology flow explains live domain relationships without looking decorative.
   - Node, edge, and label density is tuned for desktop, tablet, and mobile.
   - Refreshing state preserves prior topology content and marks freshness clearly.

   Acceptance Criteria:
   - Topology cards and connectors read as one diagram, not unrelated panels.
   - Labels do not collide or clip at reviewed viewport sizes.
   - Refreshing state keeps existing context visible while new data loads.

3. Domain signals and drill-downs
   Requirements:
   - Domain summaries use one comparable visual pattern.
   - Each signal points to the relevant domain or metrics page instead of duplicating every metric.
   - High-risk or abnormal signals are visually findable without overpowering neutral status.

   Acceptance Criteria:
   - Domain signal labels and values are comparable across all domains.
   - Drill-down actions are visible in the first dashboard pass.
   - The dashboard does not become a duplicate metrics explorer.

4. Dashboard route states and coverage
   Requirements:
   - Loading, refreshing, empty, and error states are reviewed as dashboard-specific states.
   - Error state tells the user what failed and what can still be inspected.
   - Page smoke coverage is updated for the visible dashboard priorities.

   Acceptance Criteria:
   - Empty and error states do not collapse the page into a generic message.
   - Smoke tests fail if the primary dashboard status or route title disappears.
   - Screenshot review covers desktop, tablet, mobile, and refreshing state.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx`
- Screenshot: dashboard desktop, dashboard tablet, dashboard mobile, dashboard refreshing state.

## Out Of Scope

- New broker topology APIs.
- Animated topology behavior.
- Deep metric-family browsing.
