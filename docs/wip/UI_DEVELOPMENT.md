# UI Development Guide
The Fitz admin UI is built with **Askr** (reactive SPA framework) and **Vite**.
## Directory Structure
```
ui/
├── src/
│   ├── main.tsx          # Entry point
│   ├── app.tsx           # Root component
│   ├── routes.tsx        # Route definitions
│   ├── styles.css        # Global styles
│   ├── pages/            # Page components
│   ├── components/       # Reusable components
│   └── resources/        # API data fetching
├── tests/                # Vitest tests
├── dist/                 # Built output (gitignored)
├── package.json          # Dependencies
├── vite.config.ts        # Vite configuration
└── tsconfig.json         # TypeScript config
```
## Local Development
### Option 1: With Vite Dev Server (Recommended)
Hot reload and instant updates:
```bash
# Terminal 1: Start Fitz broker
cd /path/to/fitz
cargo run
# Terminal 2: Start UI dev server
cd ui
npm install
npm run dev
```
Access at http://localhost:5173
**Proxy Configuration:**
- `/api/*` → `http://localhost:4090/api/*`
- `/metrics` → `http://localhost:4090/metrics`
- `/healthz`, `/readyz`, `/startupz` → Fitz probes
- `/ws` → `ws://localhost:4090/ws`
### Option 2: Build and Serve from Fitz
Full production simulation:
```bash
# Build UI
cd ui
npm run build
# Copy to public/
cd ..
rm -rf public/*
cp -r ui/dist/* public/
# Run Fitz
cargo run
# Access at http://localhost:4090
```
## Development Workflow
### 1. Create a New Page
```tsx
// ui/src/pages/domains.tsx
export function DomainsPage() {
  return (
    <div>
      <h1>Domains</h1>
      <p>View all domain statistics</p>
    </div>
  );
}
```
### 2. Register Route
```tsx
// ui/src/routes.tsx
import { route } from '@askrjs/askr';
import { DomainsPage } from './pages/domains';
route('/domains', DomainsPage);
```
### 3. Fetch API Data
```tsx
// ui/src/resources/domains.ts
import { resource } from '@askrjs/askr';
export interface DomainStats {
  kv: { transactions_active: number };
  stream: { streams_active: number };
  // ...
}
export const domainsResource = resource<DomainStats>(async () => {
  const response = await fetch('/api/v1/admin/stats', {
    headers: {
      'Authorization': 'Bearer ' + getToken(),
    },
  });
  if (!response.ok) throw new Error('Failed to fetch');
  return response.json();
});
```
### 4. Use in Component
```tsx
import { domainsResource } from '../resources/domains';
export function DomainsPage() {
  const stats = domainsResource.read();
  
  return (
    <div>
      <h1>Domain Statistics</h1>
      <p>KV Transactions: {stats.domains.kv.transactions_active}</p>
      <p>Active Streams: {stats.domains.stream.streams_active}</p>
    </div>
  );
}
```
## Available Scripts
```bash
npm run dev        # Start dev server (port 5173)
npm run build      # Build for production
npm run preview    # Preview production build
npm test           # Run tests with Vitest
npm run lint       # Lint with ESLint
npm run lint:fix   # Auto-fix lint issues
npm run fmt        # Format with Prettier
```
## API Integration
### Authentication
Store JWT token in localStorage:
```tsx
// ui/src/lib/auth.ts
export function setToken(token: string) {
  localStorage.setItem('fitz_token', token);
}
export function getToken(): string | null {
  return localStorage.getItem('fitz_token');
}
export function clearToken() {
  localStorage.removeItem('fitz_token');
}
```
### Authenticated Requests
```tsx
async function fetchWithAuth(url: string, options: RequestInit = {}) {
  const token = getToken();
  const headers = new Headers(options.headers);
  
  if (token) {
    headers.set('Authorization', `Bearer ${token}`);
  }
  
  return fetch(url, { ...options, headers });
}
```
### WebSocket Connection
```tsx
import { createSignal } from '@askrjs/askr';
export function useWebSocket() {
  const [connected, setConnected] = createSignal(false);
  const [socket, setSocket] = createSignal<WebSocket | null>(null);
  
  function connect() {
    const ws = new WebSocket('ws://localhost:4090/ws');
    
    ws.onopen = () => {
      setConnected(true);
      setSocket(ws);
    };
    
    ws.onclose = () => {
      setConnected(false);
      setSocket(null);
    };
    
    ws.onmessage = (event) => {
      // Handle binary message
      const data = new Uint8Array(event.data);
      console.log('Received:', data);
    };
    
    return ws;
  }
  
  return { connected, socket, connect };
}
```
## Testing
### Component Tests
```tsx
// ui/tests/components/stats.test.tsx
import { describe, it, expect } from 'vitest';
import { render } from '@askrjs/askr/test';
import { StatsCard } from '../../src/components/stats-card';
describe('StatsCard', () => {
  it('should render stats', () => {
    const { getByText } = render(() => (
      <StatsCard label="Connections" value={42} />
    ));
    
    expect(getByText('Connections')).toBeInTheDocument();
    expect(getByText('42')).toBeInTheDocument();
  });
});
```
### API Tests
```tsx
// ui/tests/resources/domains.test.ts
import { describe, it, expect, vi } from 'vitest';
import { domainsResource } from '../../src/resources/domains';
describe('domainsResource', () => {
  it('should fetch domain stats', async () => {
    global.fetch = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          domains: {
            kv: { transactions_active: 5 },
          },
        }),
      })
    );
    
    const stats = await domainsResource.load();
    expect(stats.domains.kv.transactions_active).toBe(5);
  });
});
```
## Styling
Uses **Pico CSS** for minimal, classless styling:
```tsx
// Semantic HTML = automatic styling
<article>
  <header>
    <h1>Title</h1>
  </header>
  <p>Content</p>
  <footer>
    <button>Action</button>
  </footer>
</article>
```
Custom styles in `src/styles.css`:
```css
/* Override Pico variables */
:root {
  --primary: #667eea;
  --primary-hover: #5568d3;
}
.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
}
```
## Docker Build
The Dockerfile builds the UI automatically:
```dockerfile
# Stage 1: Build UI
FROM node:20-alpine as ui-builder
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm install
COPY ui/ ./
RUN npm run build
# Stage 2: Build Fitz binary
FROM rust:1.91 as builder
# ... Rust build ...
COPY --from=ui-builder /ui/dist ./public
# Stage 3: Runtime
FROM gcr.io/distroless/cc-debian12
COPY --from=builder /usr/src/fitz/public /app/public
```
## Production Build
```bash
# Build UI
cd ui
npm run build
# Output in ui/dist/
ls -la dist/
# Docker automatically copies to public/
docker compose build
docker compose up -d
# Access at http://localhost:4090
```
## Troubleshooting
### Proxy Not Working
Check Vite is running and Fitz is on port 4090:
```bash
# Check Fitz
curl http://localhost:4090/healthz
# Check Vite proxy
curl http://localhost:5173/healthz
```
### Build Errors
```bash
# Clear caches
rm -rf ui/node_modules ui/dist
cd ui
npm install
npm run build
```
### Type Errors
```bash
# Check TypeScript
cd ui
npm run type-check
```
## Resources
- **Askr Docs**: https://github.com/askrjs/askr
- **Vite Docs**: https://vite.dev/
- **Pico CSS**: https://picocss.com/
- **Vitest**: https://vitest.dev/
