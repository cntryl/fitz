# Sprint 14: Queue Resource Detail

## Objective

Make queue resource inspection useful for operational decisions: compare, inspect inflight ownership, review dead letters, and understand recent transitions.

## Routes and Files

- `/queue/{realm}/{area}/{resource}`
- `ui/src/pages/app/queue-resource.tsx`
- `ui/src/features/queue/queue-resource-page.tsx`
- `ui/src/components/shared/queue-inflight-table.tsx`
- `ui/src/components/shared/queue-dead-letter-table.tsx`
- `ui/src/components/queue-dead-letters-panel.tsx`
- `ui/src/styles/resource.css`
- `ui/src/styles/forms.css`

## Tasks

1. Queue resource scope and risk summary
   Requirements:
   - Header clearly identifies `realm / area / resource`.
   - Summary metrics prioritize ready, delayed, inflight, dead letters, total, and oldest backlog age.
   - The first viewport makes the primary queue risk visible.

   Acceptance Criteria:
   - Users can confirm the inspected queue scope without reading raw route params.
   - The first viewport shows scope, state, and the primary queue risk.
   - Long scope values wrap or truncate intentionally.

2. Compare scopes tool
   Requirements:
   - Compare controls behave like a compact operations tool, not a marketing card.
   - Form fields align on desktop and remain usable on mobile.
   - Comparison result copy identifies both the current and compared scopes.

   Acceptance Criteria:
   - Compare controls are usable at `390px`.
   - Comparison summary is visible without pushing primary queue state too low.
   - Empty or invalid compare input does not break the page layout.

3. Inflight ownership table
   Requirements:
   - Inflight table shows owner, session, expiry, and related queue context.
   - Long owner tokens and session IDs wrap or truncate intentionally.
   - Empty inflight state is visually quiet and useful.

   Acceptance Criteria:
   - Long inflight tokens/session IDs do not break layout.
   - Expiry and ownership fields remain readable on mobile.
   - Empty inflight state does not look like an error.

4. Dead-letter review and actions
   Requirements:
   - Dead-letter table makes replay and purge decisions visually clear and consequential.
   - Destructive actions are visually distinct and require confirmation.
   - Empty dead-letter state is quiet and useful.

   Acceptance Criteria:
   - Users can identify dead-letter count, age/context, and available action.
   - Replay and purge affordances cannot be mistaken for neutral navigation.
   - Dead-letter empty state does not look like an error.

5. Timeline and resource states
   Requirements:
   - Timeline communicates recent queue transitions and whether data is derived.
   - Loading, refreshing, empty, and error states preserve resource scope context.
   - Screenshot review covers data, empty tables, compare mode, and mobile.

   Acceptance Criteria:
   - Timeline labels are dense but readable.
   - Derived data is explicitly labeled where applicable.
   - Resource state transitions do not remove scope context from the page.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx`
- Screenshot: queue resource with data, empty tables, compare mode, mobile.

## Out Of Scope

- Changing replay/purge API semantics.
- Bulk dead-letter operations.
- Queue resource creation.
