# Fitz Admin UI

The `ui/` workspace contains the Askr-based admin SPA for Fitz.

## Local Development

From `ui/`:

```bash
vp install
npm run install:browsers
vp dev
```

This starts the Vite+ dev server on `http://localhost:5173`.

Expected companion services:

- Fitz backend on `http://localhost:4090`
- Vite proxy forwards `/api`, `/metrics`, and `/ws`

For UI-only design work without a running broker, enable the Vite mock API:

```bash
VITE_FITZ_MOCK_API=1 vp dev
```

The mock server returns DTO-shaped admin payloads for the shell, overview, diagnostics, metrics,
and domain inventory/resource pages.

## Build and Verification

```bash
npm run type-check
npm run test
npm run test:e2e
npm run build
```

Production build output is written to `ui/dist/`. During Docker builds, that directory is copied directly into the final Fitz runtime image at `/app/public/`.

Local Rust builds do not embed UI assets and do not fall back to `../public`. For a production-like local run, place the contents of `ui/dist/` at `/app/public/`.

## Public Config

The browser client reads these public Vite env vars through `src/shared/config.ts`:

- `VITE_FITZ_API_BASE_URL`
- `VITE_FITZ_MOCK_API`
- `VITE_FITZ_REQUEST_TIMEOUT_MS`
- `VITE_FITZ_LOG_LEVEL`

## Local Skills

The workspace-local `skills/` docs describe the intended Askr workflow for each slice. Pick the narrowest applicable skill for the files you are changing, and treat the docs as guidance for that surface rather than a checklist to force onto unrelated code.

## UI Clarity

- [UI clarity charter](docs/ui-clarity-charter.md)
- Treat it as the visual and copy standard for new screens and feature surfaces.

## Routing

- `/` boots the SPA
- `/login` is the admin sign-in route
- `/admin` is the authenticated landing page
- `/admin/metrics` is the browser metrics explorer
- `/metrics` remains the raw Prometheus endpoint served by the broker

The SPA uses the existing admin session endpoints at `/api/v1/session`.

## Production Delivery

- Production containers ship the Fitz executable plus SPA files under `/app/public`
- The Rust HTTP layer serves filesystem assets from `/app/public`, including SPA fallback for client routes
- Asset responses preserve the existing content-type and cache-control behavior and now include ETag plus compression negotiation for supported text assets
- If `/app/public/index.html` is missing, UI entry and fallback requests return `404`

## Stack

- `@askrjs/askr` for SPA bootstrapping, routing, and state/resources
- `@askrjs/ui` for lower-level headless primitives
- `@askrjs/themes` for the default theme baseline and themed composition helpers
- `@askrjs/lucide` for icon components
- Vite+ for build, lint, format, and test tooling
