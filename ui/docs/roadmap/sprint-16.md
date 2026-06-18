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

## Requirements

- Build a repeatable screenshot capture workflow for the running app.
- Capture the route set defined in `ui/src/pages/app/_routes.tsx` and `ui/src/pages/auth/_routes.tsx`.
- Include representative mock or fixture states when a fresh broker cannot produce loaded data.
- Record visual findings in the sprint doc or a linked review note.
- Fix only regression-class issues in this sprint; page redesign belongs to earlier page sprints.

## Deliverables

- Screenshot coverage across the listed routes in desktop and mobile viewports.
- Light and dark theme passes with documented findings.
- Regression-class visual fixes only where needed to clear the review.
- Open follow-up notes for any unresolved items.

## Acceptance Criteria

- No route has horizontal page overflow at `390px`.
- No navbar/menu item clips at mobile or desktop.
- All pages have one visible page title and one `main#main-content`.
- Empty states look intentional and route-specific.
- Error states explain what failed without overpromising recovery.
- Dark mode maintains sufficient contrast for tables, cards, badges, charts, forms, and code blocks.
- Screenshot review produces no unresolved high-severity visual findings.

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
