# Sprint 08: Notice Overview

## Objective

Make the Notice overview read as live ephemeral fanout, not durable message history.

## Routes and Files

- `/notice`
- `ui/src/pages/app/notice.tsx`
- `ui/src/features/notice/*`
- Shared domain components from Sprint 06.

## Requirements

- Lead with active subscriptions, publish rate, delivery drops, and wildcard limit rejects.
- Copy describes Notice as live fanout only.
- Realm/resource inventory should point users toward active subscription scopes.
- Empty state makes clear there are no currently visible Notice realms/resources.
- Charts or comparison visuals must not imply replay or historical storage.

## Deliverables

- Notice metric ordering and labels refined.
- Header/sidebar copy audited for live fanout semantics.
- Empty/error/loading/refreshing states reviewed.
- Mobile screenshot reviewed for resource inventory and metric cards.

## Acceptance Criteria

- The first viewport answers: "Is live fanout healthy right now?"
- Copy never implies durable Notice replay.
- Subscription and publish-rate labels are visibly distinct.
- Delivery drops and wildcard rejects are easy to spot when non-zero.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: notice loaded, notice empty, notice mobile.

## Out Of Scope

- Subscription management.
- Notice resource detail redesign.
