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

## Tasks

1. Auth shell alignment
   Requirements:
   - Auth routes use the same `Header`, `Navbar`, `Container`, and layout rhythm as the app shell.
   - Brand, navigation, and account controls remain visually consistent with authenticated pages.
   - Auth content is centered by layout primitives, not page-local max-width or positioning hacks.

   Acceptance Criteria:
   - `/login` and `/logout` show the same shell spacing and brand treatment as app routes.
   - The auth card sits in a readable column without floating awkwardly in the viewport.
   - No auth-only shell clone or duplicate container rule is introduced.

2. Login form composition
   Requirements:
   - Login fields, labels, hints, and submit action use theme form primitives where available.
   - Pending and error states are compact, visible, and do not shift the form layout.
   - Copy explains the action without overstating security, durability, or session recovery.

   Acceptance Criteria:
   - Inputs and submit action align to one grid at desktop and mobile widths.
   - Pending state prevents duplicate submit while preserving user context.
   - Error state is screen-reader reachable and visually prominent.

3. Logout state handling
   Requirements:
   - Logout pending, success, and failure states use one shared card rhythm.
   - Failure copy explains what failed without promising recovery behavior the broker does not provide.
   - The route keeps enough context visible for users to understand whether they are signed out.

   Acceptance Criteria:
   - Pending, success, and failure states are visually distinct.
   - The logout card does not jump or resize dramatically between states.
   - Failure state offers the next available user action without hiding the failure.

4. Responsive and theme review
   Requirements:
   - Review `/login` and `/logout` at `390px` and desktop widths.
   - Review both routes in dark mode.
   - Update auth/page smoke coverage if visible labels or state ownership changes.

   Acceptance Criteria:
   - Button text, labels, and error text do not wrap awkwardly or overflow.
   - Dark mode maintains sufficient contrast for form fields, card surface, and status text.
   - Smoke tests still verify the user-visible route priorities.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/e2e/login.spec.ts tests/page-smoke.test.tsx`
- Screenshot: login desktop, login mobile, logout desktop, logout mobile.

## Out Of Scope

- Changing authentication policy.
- Adding provider-specific login flows.
- Broker session API changes.
