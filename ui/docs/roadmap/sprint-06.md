# Sprint 06: Shared Domain Overview Template

## Objective

Create one coherent overview page pattern for all domain pages before tuning each domain individually.

## Routes and Files

- `/lease`
- `/notice`
- `/rpc`
- `/schedule`
- `/queue`
- `/stream`
- `/kv`
- `ui/src/components/shared/domain-header.tsx`
- `ui/src/components/shared/domain-page-frame.tsx`
- `ui/src/components/shared/domain-sidebar.tsx`
- `ui/src/components/shared/domain-metric-table.tsx`
- `ui/src/components/shared/domain-realm-table.tsx`
- `ui/src/components/shared/domain-resource-browser.tsx`
- `ui/src/styles/domain.css`
- `ui/src/styles/layout.css`

## Requirements

- All domain pages use the same high-level composition: header, primary metrics, realm/resource inventory, sidebar status.
- Headers identify domain semantics, scope, freshness, and drill-down path.
- Sidebar status is useful and not a decorative duplicate of the main metrics.
- Realm/resource tables have consistent labels and empty states.
- Loading, refreshing, empty, and error states are consistent across all seven pages.
- The shared template supports domain-specific copy without forcing generic language.

## Deliverables

- Shared domain overview layout refined once and applied to all seven pages.
- Reusable state, table, metric, and sidebar patterns documented in code/tests.
- Visual rhythm fixed for pages with and without sidebars.
- Mobile collapse behavior reviewed for every domain overview.

## Acceptance Criteria

- A user can switch between any two domain pages without relearning the page structure.
- The first viewport always contains the page title, freshness/status, and at least one primary metric group.
- Sidebar remains useful but does not dominate the page.
- Empty state describes absence of visible realms/resources, not absence of historical data unless the API supports that claim.
- No page adds a custom layout wrapper that duplicates `Container`, `Stack`, `Flex`, or `Section`.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: all seven domain overviews at desktop and mobile.

## Out Of Scope

- Domain-specific metric ordering and copy, which belong to Sprints 07-13.
- Per-resource workbench redesign.
- New API endpoints.
