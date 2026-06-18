# Sprint 12: Stream Overview

## Objective

Make the Stream overview communicate durable history/replay state and watermark lag clearly.

## Routes and Files

- `/stream`
- `ui/src/pages/app/stream.tsx`
- `ui/src/features/stream/*`
- Shared domain components from Sprint 06.

## Requirements

- Lead with events total, active streams, subscriptions, operations/sec, and watermark lag.
- Copy describes Stream as durable history/replay.
- Watermark lag buckets should be understandable without a legend hunt.
- Live subscriptions must be labeled separately from durable stream metadata.
- Empty state describes no visible stream realms/resources.

## Deliverables

- Stream metric ordering revised for operator priority.
- Watermark/lag presentation polished.
- Header/sidebar copy audited for durable stream semantics.
- Mobile screenshot reviewed for lag bucket readability.

## Acceptance Criteria

- The first viewport answers: "Is durable stream history flowing and are readers caught up?"
- Watermark lag does not look like an arbitrary chart.
- Live subscription counters are not described as durable.
- Stream resource drill-down is obvious.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: stream loaded, stream empty, stream mobile.

## Out Of Scope

- Stream append/replay actions.
- Stream resource detail redesign.
