# Fitz Public Inputs

This directory is not the runtime asset root for the Fitz admin UI. The broker serves production SPA files from the fixed runtime directory `/app/public`.

## Current Role

```
public/
├── index.html          # Legacy static file; not used as a broker fallback
├── favicon.svg         # Legacy static file; not used as a broker fallback
├── openapi.yml         # Client adapter input for the UI workspace
└── README.md           # This file
```

`public/openapi.yml` remains the input for `npm run gen:adapters` in the `ui/` workspace. Checked-in files in `public/` are not embedded into the Rust binary and are not consulted when `/app/public` is missing.

## Runtime Asset Root

Production packaging must place the built SPA at `/app/public`:

- `/app/public/index.html`
- `/app/public/assets/*`
- Any other files emitted by the UI build

The Docker build runs `npm run build` in `ui/` and copies `ui/dist/` directly into the final runtime image at `/app/public/`.

## Serving Behavior

The HTTP server serves filesystem assets from `/app/public`:

- `/` -> `/app/public/index.html`
- `/assets/*` -> matching files under `/app/public/assets/`
- Existing files under `/app/public` are served directly
- Unknown non-API GET paths fall back to `/app/public/index.html`
- Path traversal is rejected
- If `/app/public/index.html` is missing, root and fallback UI requests return `404`

## Content Types

The asset server preserves the existing content-type map:

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

## Security And Caching

- No authentication is required for SPA access
- API endpoints use the `/api/v1/` prefix and keep their existing authentication behavior
- Static responses emit `Cache-Control: public, max-age=3600`
- Static responses emit `ETag`, `Vary: Accept-Encoding`, and compressed representations for supported text assets

## Development

To iterate on the admin UI:

1. Work in the `ui/` workspace
2. Run `npm run build` from `ui/` to refresh `ui/dist/`
3. Copy or package `ui/dist/` to `/app/public/` for production-like broker serving

Rebuilding the Rust binary does not refresh UI assets. If `/app/public/index.html` is unavailable, the broker returns `404` for UI entry and fallback requests.
