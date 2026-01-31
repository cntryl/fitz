# Fitz Web UI

This directory contains the Single Page Application (SPA) served at the root of the Fitz broker.

## Structure

```
public/
├── index.html          # Main SPA entry point
├── assets/             # Static assets (future)
│   ├── css/           # Stylesheets
│   ├── js/            # JavaScript bundles
│   └── img/           # Images and icons
└── README.md          # This file
```

## Routing

The HTTP server serves files from this directory with the following behavior:

- `/` → `public/index.html`
- `/assets/*` → Static files from `public/assets/`
- Any other path without file extension → `public/index.html` (SPA client-side routing)
- Paths with extensions (e.g., `/script.js`) → Exact file match or 404

## Content Types

The server automatically sets appropriate `Content-Type` headers:

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
- Static files are cached with `Cache-Control: public, max-age=3600`

## Development

To develop the SPA:

1. Edit `index.html` or add files to `public/`
2. Restart the Fitz broker
3. Navigate to `http://localhost:8080/`

For production builds with bundlers (Vite, Webpack, etc.):

```bash
# Build your SPA
npm run build

# Copy output to public/
cp -r dist/* public/
```

## Current Features

The default `index.html` provides:

- Broker status indicator (checks `/healthz`)
- Feature overview
- Links to metrics and admin API
- Responsive design
- No external dependencies (vanilla JS + CSS)

## Future Enhancements

Planned additions:

- [ ] Real-time metrics dashboard
- [ ] Domain statistics visualization
- [ ] WebSocket connection monitor
- [ ] Realm browser
- [ ] Message tracing UI
- [ ] Configuration editor
