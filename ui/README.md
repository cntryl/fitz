# Fitz Admin UI

The `ui/` workspace contains the Askr-based admin SPA for Fitz.

## Local Development

From `ui/`:

```bash
npm install
npm run dev
```

This starts the Vite dev server on `http://localhost:5173`.

Expected companion services:

- Fitz backend on `http://localhost:4090`
- Vite proxy forwards `/api`, `/metrics`, `/healthz`, `/readyz`, `/startupz`, and `/ws`

## Build and Verification

```bash
npm run build
npm run type-check
npm run lint
npm test -- --run
```

Production build output is written to `../public`, which is then served by the Rust HTTP server at `/`.

## Routing

- `/` boots the SPA
- `/login` is the admin sign-in route
- `/admin` is the authenticated landing page

The SPA uses the existing admin session endpoints at `/api/v1/session`.

## Stack

- `@askrjs/askr` for SPA bootstrapping, routing, and state/resources
- `@askrjs/askr-ui` for headless UI primitives
- `@askrjs/askr-themes` for the default theme baseline
- `@askrjs/icons-lucide` for icon components
- Vite + TypeScript + Vitest for build and test tooling
