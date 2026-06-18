# Sprint 04: Sessions

## Objective

Make active broker sessions easy to scan, filter mentally, and diagnose.

## Routes and Files

- `/sessions`
- `ui/src/pages/app/sessions.tsx`
- `ui/src/components/shared/session-table.tsx`
- `ui/src/styles/domain.css`

## Tasks

1. Session posture summary
   Requirements:
   - The page header states active session posture and freshness.
   - Summary values identify count, route families, transports, and idle risk.
   - Summary copy avoids implying historical recovery or durable session continuity.

   Acceptance Criteria:
   - The first viewport shows the page title, freshness, summary, and beginning of detail.
   - Route family and transport signals are visually distinct.
   - Empty summary values read as current broker state, not historical absence.

2. Session table semantics
   Requirements:
   - The table is compact, readable, and horizontally safe.
   - Identity fields distinguish `subject`, `identity_claim`, `identity_value`, and `route_family`.
   - Session ID and remote address use intentional wrapping or truncation.

   Acceptance Criteria:
   - Long session IDs and remote addresses do not blow out page width.
   - Identity and route-family fields are not visually or textually conflated.
   - Copy does not equate `realm` and `route family`.

3. Empty and error states
   Requirements:
   - Empty state makes it clear that no sessions are currently connected.
   - Error state explains which session data failed to load.
   - State surfaces preserve the page frame and summary rhythm.

   Acceptance Criteria:
   - Empty state does not imply stored session history exists.
   - Error state remains route-specific and actionable.
   - Loading, empty, and error states do not cause layout jumps.

4. Mobile table behavior
   Requirements:
   - Review the sessions route at `390px`.
   - Choose wrapping, scrolling, or reduced columns intentionally for the table.
   - Preserve visible labels for identity, route family, and remote address on mobile.

   Acceptance Criteria:
   - The page has no horizontal overflow outside the intended table behavior.
   - Users can still identify session identity and route family on mobile.
   - Screenshot review covers data, empty, and mobile states.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx`
- Screenshot: sessions with data, sessions empty, sessions mobile.

## Out Of Scope

- Session mutation actions.
- Session detail route.
- New filtering API.
