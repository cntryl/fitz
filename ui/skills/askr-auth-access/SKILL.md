---
name: askr-auth-access
description: Use when building Askr authentication, session loading, public/app route branches, protected layouts, role and permission metadata, redirects, login/logout, and access-denied UX.
---

# Askr Auth Access

Use this for authentication and authorization in route-first Askr apps. The goal is explicit route policy, explicit session resolution, and no token or permission logic leaking into pages.

## Inspect First

- `src/pages/_routes.tsx`, `src/pages/public/_routes.tsx`, and `src/pages/app/_routes.tsx`
- `src/pages/public/_layout.tsx` and `src/pages/app/_layout.tsx`
- Existing session, token, and user helpers in `src/shared` or `src/features/auth`
- Router auth resolver configuration

## Use This When

- You need guest-only and authenticated route branches.
- You need route-level auth or permission metadata.
- You need login, logout, redirect, or forbidden behavior.
- You need to keep access checks out of page-local component logic.

## Do This In Order

1. Keep public and authenticated branches explicit in the route tree.
2. Put `auth`, role, or permission metadata on the narrowest route group or route that owns the policy.
3. Resolve session state before rendering protected data or destructive controls.
4. Redirect unauthenticated users to login with a return target when useful.
5. Show a signed-in forbidden state when the user is authenticated but lacks permission.
6. Keep token storage, refresh, and header policy in auth helpers or adapters, not components.

## Copy This Shape

```tsx
group({ layout: PublicLayout, auth: "guest" }, () => {
  registerPublicRoutes();
});

group({ layout: AppLayout, auth: true }, () => {
  registerAppRoutes();
});
```

## Never Do These

- Per-page auth checks duplicated across protected routes.
- Rendering protected app data before session resolution.
- Putting token storage or API auth header logic in components.
- Treating roles and permissions as visual-only state.
- Client-only authorization decisions for sensitive server actions.
- Silent redirects when the user is signed in but lacks access.

## Validate

- Public and app branches are explicit.
- Protected routes have auth policy in route metadata.
- Access-denied, loading, and redirect behavior are tested.
- Auth state is available to adapters without leaking transport details into UI.

## Done When

- Session resolution is explicit before protected data renders.
- Auth and authorization live in route metadata or auth workflows, not scattered through pages.
- Unauthorized, redirect, and signed-out states are all covered.
- Sensitive transport or token logic did not leak into UI components.

## Handoff

- Use `askr-routing-layouts` when auth changes also reshape the route tree.
- Use `askr-api-integration` when auth headers, session refresh, or adapter policy is changing.
- Use `askr-observability-debugging` when denial reasons or audit trails must stay visible.
