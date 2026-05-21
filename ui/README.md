# Fitz Admin UI

The `ui/` workspace contains the Askr-based admin SPA for Fitz.

## Local Development

From `ui/`:

```bash
vp install
vp dev
```

This starts the Vite+ dev server on `http://localhost:5173`.

Expected companion services:

- Fitz backend on `http://localhost:4090`
- Vite proxy forwards `/api`, `/metrics`, `/healthz`, `/readyz`, `/startupz`, and `/ws`

## Build and Verification

```bash
vp check
vp test
vp build
```

Production build output is written to `../public`, which is then served by the Rust HTTP server at `/`.

## Routing

- `/` boots the SPA
- `/login` is the admin sign-in route
- `/admin` is the authenticated landing page

The SPA uses the existing admin session endpoints at `/api/v1/session`.

## Stack

- `@askrjs/askr` for SPA bootstrapping, routing, and state/resources
- `@askrjs/ui` for lower-level headless primitives
- `@askrjs/themes` for the default theme baseline and themed composition helpers
- `@askrjs/lucide` for icon components
- Vite+ for build, lint, format, and test tooling
