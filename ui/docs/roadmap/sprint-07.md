# Sprint 07: Lease Overview

## Objective

Make the Lease overview communicate ephemeral ownership coordination clearly and compactly.

## Routes and Files

- `/lease`
- `ui/src/pages/app/lease.tsx`
- `ui/src/features/lease/*`
- Shared domain components from Sprint 06.

## Requirements

- Lead with active leases, waiter depth, oldest lease age, and ownership pressure.
- Copy describes leases as ephemeral ownership coordination.
- Invalid token rejects, forced releases, and acquire timeouts are visible as risk signals.
- Realm inventory makes it obvious which lease scopes are currently visible.
- Empty state states that no lease realms are visible in the current broker snapshot.

## Deliverables

- Lease metric ordering revised for operator priority.
- Header and sidebar copy audited for semantic accuracy.
- Empty/error/loading/refreshing states reviewed.
- Mobile screenshot reviewed for metric readability.

## Acceptance Criteria

- The first viewport answers: "Are there active owners or waiters?"
- Copy does not imply durable ownership continuity after disconnect/restart.
- Oldest lease age has a readable unit and does not look like a timestamp.
- Sidebar adds diagnostic context instead of repeating the metric table.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: lease loaded, lease empty, lease mobile.

## Out Of Scope

- Lease mutation actions.
- Lease resource detail redesign.
