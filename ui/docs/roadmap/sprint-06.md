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

## Tasks

1. Shared overview frame
   Requirements:
   - All domain pages use the same high-level composition: header, primary metrics, inventory, and sidebar status.
   - The frame is built from Askr layout primitives before app-local wrappers.
   - Pages with and without sidebars keep the same visual rhythm.

   Acceptance Criteria:
   - Users can switch between any two domain pages without relearning the layout.
   - No page adds a custom wrapper that duplicates `Container`, `Stack`, `Flex`, or `Section`.
   - Desktop, tablet, and mobile layouts collapse without page overflow.

2. Domain header and state model
   Requirements:
   - Headers identify domain semantics, scope, freshness, and drill-down path.
   - Loading, refreshing, empty, and error states are consistent across all seven overview pages.
   - State copy stays domain-specific and avoids generic broker-empty language where scope matters.

   Acceptance Criteria:
   - The first viewport always contains page title, freshness/status, and at least one primary metric group.
   - Empty state describes absence of visible realms/resources, not historical absence.
   - Error state tells the user which domain overview failed.

3. Metrics and inventory tables
   Requirements:
   - Metric, realm, and resource tables use consistent labels and density.
   - Inventory rows make drill-down paths obvious where a resource route exists.
   - Long realm, area, and resource values wrap or truncate intentionally.

   Acceptance Criteria:
   - Metric tables remain comparable across domains.
   - Resource inventory is usable at desktop and mobile widths.
   - No table creates horizontal page overflow outside intended scroll regions.

4. Sidebar behavior
   Requirements:
   - Sidebar status adds useful diagnostic context and does not duplicate the main metrics.
   - Sidebar width and sticky behavior are handled once in the shared frame.
   - Sidebar collapses cleanly below the desktop layout.

   Acceptance Criteria:
   - Sidebar remains useful but does not dominate the page.
   - Sidebar content stacks after primary content on mobile.
   - Pages without meaningful sidebar data still look intentional.

5. Shared coverage
   Requirements:
   - Add or update shared tests for domain overview structure and query states.
   - Review all seven domain overviews in desktop and mobile screenshots.
   - Keep domain-specific metric ordering for sprints 07-13.

   Acceptance Criteria:
   - Tests fail if a domain page loses the shared frame or primary state surface.
   - Screenshot review confirms consistent rhythm across all seven domain pages.
   - No domain-specific copy rewrite is required to complete this shared template sprint.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: all seven domain overviews at desktop and mobile.

## Out Of Scope

- Domain-specific metric ordering and copy, which belong to Sprints 07-13.
- Per-resource workbench redesign.
- New API endpoints.
