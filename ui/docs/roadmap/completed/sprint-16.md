# Sprint 16: Final Responsive, Dark Mode, And Visual QA

## Objective

Close the UI sprint series with a full visual review and regression gate across routes, states, viewport sizes, and color modes.

## Routes and Files

- `/`
- `/admin`
- `/sessions`
- `/admin/metrics`
- `/lease`
- `/notice`
- `/rpc`
- `/schedule`
- `/queue`
- `/stream`
- `/kv`
- `/queue/{realm}/{area}/{resource}`
- `/kv/{realm}/{area}/{resource}`
- `/lease/{realm}/{area}/{resource}`
- `/notice/{realm}/{area}/{resource}`
- `/rpc/{realm}/{area}/{resource}`
- `/schedule/{realm}/{area}/{resource}`
- `/stream/{realm}/{area}/{resource}`
- `/login`
- `/logout`
- `ui/src/pages/app/_routes.tsx`
- `ui/src/pages/auth/_routes.tsx`
- `ui/tests/e2e/shell.spec.ts`
- `ui/tests/page-smoke.test.tsx`

## Tasks

1. Screenshot capture workflow
   Requirements:
   - Build or verify a repeatable screenshot capture workflow for the running app.
   - Capture the route set defined in `ui/src/pages/app/_routes.tsx` and `ui/src/pages/auth/_routes.tsx`.
   - Include representative mock or fixture states when a fresh broker cannot produce loaded data.

   Acceptance Criteria:
   - The screenshot workflow can be rerun without manual route discovery.
   - Captures cover app routes, auth routes, and resource detail routes.
   - Missing fixture data is recorded as a review limitation or follow-up.

2. Responsive layout audit
   Requirements:
   - Review every listed route at mobile and desktop widths.
   - Verify top navbar, mobile nav, and domain dropdown do not clip or overlap content.
   - Verify every page has one visible page title and one `main#main-content`.

   Acceptance Criteria:
   - No route has horizontal page overflow at `390px`.
   - No navbar or menu item clips at mobile or desktop.
   - Page title and main landmark checks pass for every route.

3. Light and dark theme audit
   Requirements:
   - Review tables, cards, badges, charts, forms, code blocks, menus, and overlays in light and dark themes.
   - Record any contrast, surface, or z-index issues found during review.
   - Fix only regression-class issues in this sprint.

   Acceptance Criteria:
   - Dark mode maintains sufficient contrast for dense operational surfaces.
   - Dropdowns, overlays, and sticky chrome layer above page content correctly.
   - No unresolved high-severity theme or z-index finding remains.

4. State quality audit
   Requirements:
   - Review loading, refreshing, empty, error, and loaded states where supported.
   - Empty states remain route-specific and intentional.
   - Error states explain what failed without overpromising recovery.

   Acceptance Criteria:
   - No route collapses to a generic loading or empty message when a route-specific state is expected.
   - Refreshing states preserve previous content where the route supports it.
   - Error copy stays within Fitz semantics and supported behavior.

5. Regression closure
   Requirements:
   - Record visual findings in the sprint doc or a linked review note.
   - Fix only regression-class issues needed to clear final QA.
   - Move unresolved redesign ideas into future work instead of expanding this sprint.

   Acceptance Criteria:
   - Screenshot review produces no unresolved high-severity visual findings.
   - Any unresolved non-blocking findings are documented with route, viewport, theme, and state.
   - Final validation commands pass before the sprint is moved to `completed/`.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test`
- `npm run build`
- `docker compose up -d --build`
- Browser screenshot pass against `http://127.0.0.1:4090/`.

## Out Of Scope

- New feature work.
- New visual direction.
- Broad refactors not needed to close visual regressions.
