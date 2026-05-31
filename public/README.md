# Fitz Embedded UI Inputs

This directory is no longer a runtime dependency for the Fitz admin UI. Production assets are embedded into the Fitz executable at build time, and the running broker serves them directly from memory.

## Current Role

```
public/
├── index.html          # Last-resort local fallback when ui/dist is unavailable
├── favicon.svg         # Ancillary static file kept in the repo
├── openapi.yml         # Client adapter input for the UI workspace
└── README.md           # This file
```

The preferred production asset source is `ui/dist/`. During Docker builds, those generated assets are copied into the Rust build stage and embedded into the binary.

## Serving Behavior

The HTTP server now serves embedded assets with the same public route contract as before:

- `/` → embedded `index.html`
- `/assets/*` → embedded static assets from the production UI build
- Any other path that does not resolve to an embedded file falls back to embedded `index.html`
- Path traversal is rejected

## Content Types

The embedded asset server preserves the existing content-type map:

| Extension | Content-Type |
|-----------|-------------|
| `.html` | `text/html; charset=utf-8` |
| `.css` | `text/css; charset=utf-8` |
| `.js` | `application/javascript; charset=utf-8` |
| `.json` | `application/json` |
| `.png` | `image/png` |
| `.jpg`, `.jpeg` | `image/jpeg` |
| `.svg` | `image/svg+xml` |
| `.ico` | `image/x-icon` |
| `.woff`, `.woff2`, `.ttf` | Font types |

## Security

- No authentication required for SPA access (public)
- API endpoints use `/api/v1/` prefix and require authentication
- Embedded responses preserve `Cache-Control: public, max-age=3600`
- Embedded responses now also emit `ETag`, `Vary: Accept-Encoding`, and compressed representations for supported text assets

## Development

To iterate on the admin UI:

1. Work in the `ui/` workspace
2. Run `npm run build` from `ui/` to refresh `ui/dist/`
3. Rebuild Fitz so the new production assets are embedded into the executable

If `ui/dist/` is unavailable during a local Rust build, Fitz falls back to embedding the checked-in `public/` directory so the binary still compiles.

This fallback is for developer convenience only. Production containers no longer ship a `/app/public` tree.
