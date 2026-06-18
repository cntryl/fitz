# Sprint 13: KV Overview

## Objective

Make the KV overview communicate current authoritative state and transaction pressure without implying event history.

## Routes and Files

- `/kv`
- `ui/src/pages/app/kv.tsx`
- `ui/src/features/kv/*`
- Shared domain components from Sprint 06.

## Tasks

1. KV metric priority
   Requirements:
   - Lead with keys total, active transactions, operations/sec, commit failures, rollbacks, and invalid transaction rejects.
   - Transaction counters are visually distinct from key count.
   - Commit failures, rollbacks, and invalid transaction rejects are findable when non-zero.

   Acceptance Criteria:
   - The first viewport answers: "Is current state active and are transactions failing?"
   - Transaction pressure does not read as neutral inventory count.
   - Primary metrics fit the shared overview rhythm from Sprint 06.

2. KV semantic copy
   Requirements:
   - Header, sidebar, and state copy describe KV as current authoritative state.
   - Copy does not describe KV as durable history or replay.
   - Active transactions are labeled as broker-local/session-scoped where applicable.

   Acceptance Criteria:
   - No visible copy uses stream, replay, or event-history language for KV.
   - Empty state describes no visible KV realms/resources.
   - Transaction scope language is precise and not overstated.

3. KV inventory and sidebar
   Requirements:
   - Realm and resource inventory points toward current state scopes.
   - Sidebar adds transaction or current-state context instead of repeating the metric table.
   - Resource drill-down path is obvious.

   Acceptance Criteria:
   - Users can identify which KV resource to inspect next.
   - Sidebar context helps interpret transaction pressure.
   - Long key/resource labels do not overflow on mobile.

4. KV states and screenshots
   Requirements:
   - Review loading, refreshing, empty, and error states for KV-specific language.
   - Review transaction counter readability at mobile width.
   - Update page smoke or query tests if visible priorities change.

   Acceptance Criteria:
   - Empty and error states are route-specific and preserve current-state semantics.
   - Mobile layout keeps key count and transaction signals readable.
   - Screenshot review covers kv loaded, kv empty, and kv mobile.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: kv loaded, kv empty, kv mobile.

## Out Of Scope

- KV mutation tools.
- KV resource detail redesign.
