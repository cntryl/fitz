# Sprint 00: UI Quality Operating Model

## Summary

Sprint 00 sets the delivery system for the rest of Fitz Admin UI work. It does not change app behavior; it defines the visual and UX rules, review gates, and sprint template that later page and layout sprints must follow.

Fitz Admin should read as an operational broker console: dense, calm, and scannable. It should not drift toward a landing page, a template gallery, or a pile of unrelated cards.

## Key Changes

- Establish one shared UI standard for Fitz Admin: dense operational layouts, truthful state handling, and strict use of Askr theme primitives.
- Define the sprint template every later page/layout sprint must use: objective, scope/files, requirements, deliverables, acceptance criteria, validation, and out of scope.
- Lock the core visual gates:
  - no overflow at mobile, tablet, or desktop widths
  - clear page title, status, freshness, and next action
  - distinct loading, refreshing, empty, and error states
  - keyboard-reachable navigation with stable accessible names
- Require screenshot-based review for shell, dashboard, dense tables, empty states, and resource detail views.
- Preserve Fitz copy semantics: `realm`, `route family`, live/ephemeral state, durable history, and current authoritative state must be used precisely.

## Test Plan

- Review each later sprint doc against the standard template defined in sprint 00.
- Verify every route in `ui/src/pages/app/_routes.tsx` and `ui/src/pages/auth/_routes.tsx` is assigned to at least one sprint.
- Use the sprint 00 acceptance gates as the checklist for later UI work.
- For implementation work that follows, require the standard UI validation set: `type-check`, `lint`, `test`, and `build`.

## Assumptions

- Sprint 00 is a roadmap and process sprint, not an app code sprint.
- The current Askr theme stack remains the UI baseline.
- No new design system or custom shell layer is introduced here.
- `ui/docs/ui-clarity-charter.md` remains the product-quality authority.
