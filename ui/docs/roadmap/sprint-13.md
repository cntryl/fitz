# Sprint 13: KV Overview

## Objective

Make the KV overview communicate current authoritative state and transaction pressure without implying event history.

## Routes and Files

- `/kv`
- `ui/src/pages/app/kv.tsx`
- `ui/src/features/kv/*`
- Shared domain components from Sprint 06.

## Requirements

- Lead with keys total, active transactions, operations/sec, commit failures, rollbacks, and invalid transaction rejects.
- Copy describes KV as current authoritative state.
- Active transactions are labeled as broker-local/session-scoped when applicable.
- Realm/resource inventory should point toward current state scopes.
- Empty state describes no visible KV realms/resources.

## Deliverables

- KV metric ordering revised for operator priority.
- Header/sidebar copy audited for current-state semantics.
- Empty/error/loading/refreshing states reviewed.
- Mobile screenshot reviewed for transaction counter readability.

## Acceptance Criteria

- The first viewport answers: "Is current state active and are transactions failing?"
- Copy does not describe KV as durable history or replay.
- Transaction counters are visibly different from key count.
- Resource drill-down path is obvious.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: kv loaded, kv empty, kv mobile.

## Out Of Scope

- KV mutation tools.
- KV resource detail redesign.
