# Sprint 04: Sessions

## Objective

Make active broker sessions easy to scan, filter mentally, and diagnose.

## Routes and Files

- `/sessions`
- `ui/src/pages/app/sessions.tsx`
- `ui/src/components/shared/session-table.tsx`
- `ui/src/styles/domain.css`

## Requirements

- Page header states active session posture and freshness.
- Summary values identify count, route families, transports, and idle risk.
- Session table is compact, readable, and horizontally safe.
- Identity fields distinguish `subject`, `identity_claim`, `identity_value`, and `route_family`.
- Empty state makes it clear that no sessions are currently connected.
- Error state does not imply historical recovery data exists.

## Deliverables

- Sessions summary hierarchy refined.
- Table column order and labels reviewed for operator workflow.
- Mobile table behavior reviewed and corrected.
- Empty and error states polished.

## Acceptance Criteria

- Session ID and remote address do not blow out the layout.
- Route family and identity values are visually distinct.
- Table remains usable on mobile through wrapping, scrolling, or reduced columns.
- First viewport shows summary plus the beginning of session detail.
- Copy does not equate `realm` and `route family`.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx`
- Screenshot: sessions with data, sessions empty, sessions mobile.

## Out Of Scope

- Session mutation actions.
- Session detail route.
- New filtering API.
