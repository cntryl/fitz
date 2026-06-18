# Sprint 02: Auth Layout, Login, And Logout

## Objective

Make authentication screens feel like part of Fitz Admin instead of isolated cards with generic copy.

## Routes and Files

- `/login`
- `/logout`
- `ui/src/pages/auth/_layout.tsx`
- `ui/src/pages/auth/login.tsx`
- `ui/src/pages/auth/logout.tsx`
- `ui/src/styles/forms.css`

## Requirements

- Auth layout uses `Header`, `Navbar`, `Container`, and `Flex`.
- Login and logout cards are centered without feeling detached from the app shell.
- Copy explains what is happening without overstating security or durability.
- Form controls use theme field/input/button primitives where available.
- Error and pending states are visible, compact, and not layout-shifting.
- Logout pending and failure states are visually distinct.

## Deliverables

- Auth page header aligned with app shell brand styling.
- Login form spacing, labels, hints, and submit button refined.
- Logout card refined for pending, success, and failure states.
- Mobile layout reviewed at `390px`.
- Dark mode screenshot reviewed.

## Acceptance Criteria

- Login card does not exceed comfortable reading width.
- Inputs and button align to the same grid.
- Pending state disables duplicate submit without hiding context.
- Error state is reachable by screen reader and visually prominent.
- Text does not wrap awkwardly inside buttons or field labels.
- Auth pages do not introduce page-specific shell classes when Askr layout props are enough.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/e2e/login.spec.ts tests/page-smoke.test.tsx`
- Screenshot: login desktop, login mobile, logout desktop, logout mobile.

## Out Of Scope

- Changing authentication policy.
- Adding provider-specific login flows.
- Broker session API changes.
