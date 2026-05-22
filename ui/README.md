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
npm run type-check
npm run test
npm run build
```

Production build output is written to `../public`, which is then served by the Rust HTTP server at `/`.

## Public Config

The browser client reads these public Vite env vars through `src/shared/config.ts`:

- `VITE_FITZ_API_BASE_URL`
- `VITE_FITZ_REQUEST_TIMEOUT_MS`
- `VITE_FITZ_LOG_LEVEL`

## Local Skills

The repo-local `.skills/` docs describe the intended Askr workflow for each slice. Pick the narrowest applicable skill for the files you are changing, and treat the docs as guidance for that surface rather than a checklist to force onto unrelated code.

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
