# Sprint 12: Stream Overview

## Objective

Make the Stream overview communicate durable history/replay state and watermark lag clearly.

## Routes and Files

- `/stream`
- `ui/src/pages/app/stream.tsx`
- `ui/src/features/stream/*`
- Shared domain components from Sprint 06.

## Tasks

1. Stream metric priority
   Requirements:
   - Lead with events total, active streams, subscriptions, operations/sec, and watermark lag.
   - Watermark lag buckets are understandable without a legend hunt.
   - Live subscriptions are labeled separately from durable stream metadata.

   Acceptance Criteria:
   - The first viewport answers: "Is durable stream history flowing and are readers caught up?"
   - Watermark lag does not look like an arbitrary chart.
   - Primary metrics fit the shared overview rhythm from Sprint 06.

2. Stream semantic copy
   Requirements:
   - Header, sidebar, and state copy describe Stream as durable history/replay.
   - Live subscription counters are not described as durable.
   - Copy separates stream metadata from reader/session activity.

   Acceptance Criteria:
   - Durable history and live subscription language are visibly distinct.
   - Empty state describes no visible stream realms/resources.
   - No visible copy confuses reader state with stored event history.

3. Stream inventory and sidebar
   Requirements:
   - Realm and resource inventory points toward streams with activity or lag.
   - Sidebar adds replay/watermark context instead of repeating the metric table.
   - Stream drill-down path is obvious.

   Acceptance Criteria:
   - Users can identify which stream resource needs inspection.
   - Sidebar context helps interpret lag and activity.
   - Inventory rows remain readable on mobile.

4. Stream states and screenshots
   Requirements:
   - Review loading, refreshing, empty, and error states for Stream-specific language.
   - Review lag bucket readability at mobile width.
   - Update page smoke or query tests if visible priorities change.

   Acceptance Criteria:
   - Empty and error states are route-specific and preserve durable-stream semantics.
   - Mobile layout keeps lag and activity signals readable.
   - Screenshot review covers stream loaded, stream empty, and stream mobile.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/queries.test.ts`
- Screenshot: stream loaded, stream empty, stream mobile.

## Out Of Scope

- Stream append/replay actions.
- Stream resource detail redesign.
