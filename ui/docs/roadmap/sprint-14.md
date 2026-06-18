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

## Requirements

- Header clearly identifies the queue scope: `realm / area / resource`.
- Summary metrics prioritize ready, delayed, inflight, dead letters, total, and oldest backlog age.
- Compare scopes UI is compact and behaves like a tool, not a marketing card.
- Inflight table shows owner/session/expiry without overflow.
- Dead-letter table makes replay and purge decisions visually clear and consequential.
- Timeline communicates recent queue transitions and whether data is derived.
- Empty inflight/dead-letter states are visually quiet and useful.

## Deliverables

- Queue resource page hierarchy revised around operational tasks.
- Compare form fields aligned and responsive.
- Inflight and dead-letter tables polished for long IDs.
- Replay/purge confirmation affordances reviewed.
- Timeline density and labels corrected.

## Acceptance Criteria

- The first viewport shows scope, state, and the primary queue risk.
- Destructive actions are visually distinct and require confirmation.
- Long inflight tokens/session IDs do not break layout.
- Compare controls are usable on mobile.
- Dead-letter empty state does not look like an error.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx`
- Screenshot: queue resource with data, empty tables, compare mode, mobile.

## Out Of Scope

- Changing replay/purge API semantics.
- Bulk dead-letter operations.
- Queue resource creation.
