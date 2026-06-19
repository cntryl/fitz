# Sprint 00: UI Quality Operating Model

## Objective

Define the delivery workflow every active Fitz Admin UI sprint must follow: small task cards, clear requirements, concrete acceptance criteria, visual review, and a consistent done marker.

## Routes and Files

- `ui/docs/roadmap/sprint-*.md`
- `ui/docs/roadmap/completed/`
- `ui/docs/ui-clarity-charter.md`
- UI route files under `ui/src/pages/app/` and `ui/src/pages/auth/`

## Tasks

1. Define the sprint task-card standard
   Requirements:
   - Every active implementation sprint uses `## Tasks` instead of broad top-level `Deliverables` and `Acceptance Criteria`.
   - Each task is small enough to implement, review, and validate independently.
   - Each task includes local `Requirements` and `Acceptance Criteria`.
   - Task wording names the user-facing surface, behavior, or state being improved.

   Acceptance Criteria:
   - Active sprint docs can be scanned task-by-task without cross-reading separate deliverable and acceptance sections.
   - An implementer can pick one task and know what must change and how done is judged.
   - Completed sprint docs are not rewritten unless explicitly reopened.

2. Lock Fitz Admin visual and semantic gates
   Requirements:
   - Sprint tasks preserve the operational-console direction: dense, calm, scannable, and route-specific.
   - Tasks require truthful Fitz semantics for `realm`, `route family`, live/ephemeral state, durable history, and current authoritative state.
   - Tasks point implementers toward Askr layout, theme, routing, and UI primitives before app-local wrappers.
   - Tasks include loading, refreshing, empty, error, and loaded states where the route owns those states.

   Acceptance Criteria:
   - No sprint asks for landing-page composition, decorative card piles, or unsupported durability/recovery claims.
   - Route and domain copy requirements match Fitz domain meanings.
   - Framework-owned layout, theme, menu, focus, and routing behavior remains the default implementation path.

3. Define review and completion workflow
   Requirements:
   - Active sprint validation lists the smallest useful command set for that sprint.
   - Visual tasks name the required screenshot or browser review states.
   - A sprint is marked done by moving its file into `ui/docs/roadmap/completed/`.
   - The final QA sprint verifies all active route, theme, and viewport coverage before closing the roadmap series.

   Acceptance Criteria:
   - Each active sprint has explicit validation commands or screenshot checks.
   - Done state is represented by file location, not an inline status checkbox.
   - Sprint 16 can audit the route set without inventing new page redesign scope.

## Validation

- `rg -n "^## (Deliverables|Acceptance Criteria)$" ui/docs/roadmap/sprint-*.md` returns no active sprint matches.
- `rg -n "^## Tasks$" ui/docs/roadmap/sprint-*.md` shows every active sprint has a task section.
- Review active sprint headings for the standard workflow shape.

## Out Of Scope

- Implementing UI code.
- Reopening completed sprint 01.
- Changing Fitz product semantics or API behavior.
