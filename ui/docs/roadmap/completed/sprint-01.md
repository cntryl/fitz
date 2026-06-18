# Sprint 01: App Shell, Navbar, And Page Frame

## Objective

Split the global UI chrome work into small, finishable deliverables so each piece has one owner, one purpose, and clear acceptance criteria.

## Routes and Files

- `ui/src/pages/app/_layout.tsx`
- `ui/src/pages/auth/_layout.tsx`
- `ui/src/components/shared/domain-page-frame.tsx`
- `ui/src/styles/base.css`
- `ui/src/styles/layout.css`
- `ui/src/styles/responsive.css`
- `ui/src/styles/shell.css`
- `ui/tests/shared-ui-polish.test.tsx`
- `ui/tests/page-smoke.test.tsx`
- `ui/tests/e2e/shell.spec.ts`

## Deliverables

1. Shell primitives and container ownership
   Requirements:
   - App chrome uses `Header`, `Navbar`, `NavBrand`, `NavGroup`, `NavLink`, `Dropdown`, and `Container`.
   - Main content width is owned by `Container`, not page-local max-width hacks.
   - Route content exposes exactly one `main#main-content`.
   - Skip link targets `#main-content`.
   - No app-local shell clone is introduced unless a framework gap makes it unavoidable.

   Acceptance Criteria:
   - Header and content use the same container rhythm on desktop and mobile.
   - `main#main-content` is present once per route render.
   - The skip link lands on the main content landmark.
   - Page content starts below the header with consistent spacing on all core routes.
   - There is no duplicate container logic hidden in page-specific styles.

2. Navbar structure and domain dropdown
   Requirements:
   - Top navbar includes Fitz Admin brand, Dashboard, Sessions, Metrics, Domains, Theme toggle, and Sign out.
   - Domains opens a framework dropdown containing Lease, Notice, RPC, Schedule, Queue, Stream, and KV.
   - Dropdown is keyboard reachable and uses the framework portal/overlay behavior.
   - Active and hover states follow the AskR theme, not ad hoc CSS.
   - Mobile navbar remains usable without overlap, clipping, or hidden controls.

   Acceptance Criteria:
   - Desktop navbar fits at `1024px` without account actions dropping into the content area.
   - The Domains menu opens above page content and does not clip behind the sticky header.
   - Each dropdown item is readable, aligned, and clickable.
   - Route activation closes the dropdown.
   - Long username text truncates without pushing adjacent controls offscreen.
   - Mobile layout exposes all global navigation and account actions in a usable arrangement.

3. Shared page frame
   Requirements:
   - Sidebar pages use one shared two-column frame.
   - The frame is built on Askr layout primitives.
   - Sidebar width, content width, and sticky behavior are handled once in the shared frame.
   - The frame collapses cleanly on tablet and mobile.

   Acceptance Criteria:
   - Sidebar and content align consistently across domain pages that use the frame.
   - Desktop layout preserves readable sidebar width and stable main content width.
   - Tablet and mobile collapse produces a single-column experience without overflow.
   - No page-specific frame variant duplicates the shared layout logic.

4. Visual polishing and regression coverage
   Requirements:
   - Add or update tests for top nav links, domain menu entries, skip link, and the single main landmark.
   - Add screenshot-based review for dashboard desktop, dashboard mobile with nav open, and desktop domain dropdown open.
   - Validate the shell in both light and dark theme states.
   - Keep the focus on layout and chrome only; do not widen into content rewrites.

   Acceptance Criteria:
   - Tests fail if the navbar loses required destinations or the dropdown items disappear.
   - Tests fail if the skip link or main landmark is missing.
   - Screenshot review shows no overlap, clipping, or unreadable text in the shell.
   - Light and dark themes both remain legible in the navbar and dropdown.
   - The sprint can be marked done without any unresolved shell-level visual defects.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/shared-ui-polish.test.tsx tests/page-smoke.test.tsx`
- `npm run test:e2e -- tests/e2e/shell.spec.ts`
- `npm run build`

## Out Of Scope

- Page-specific content hierarchy.
- Rewriting domain tables, charts, or dashboard panels.
- Auth behavior beyond visible chrome consistency.
