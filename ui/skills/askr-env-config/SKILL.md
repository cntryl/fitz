---
name: askr-env-config
description: Use when configuring Askr apps with environment variables, API base URLs, feature flags, deployment config, local mocks, generated client configuration, and separating config from runtime state.
---

# Askr Env Config

Use this for environment-specific application configuration. The goal is one typed config boundary, explicit public values, and no env reads scattered through UI code.

## Use This When

- You need API base URLs, feature flags, deployment config, or client configuration.
- The app has local mocks, staging behavior, or environment-specific stream endpoints.
- You need missing required config to fail early.
- You want tests to override config deterministically.

## Inspect First

- Existing Vite or runtime environment usage and app config helpers
- Generated API client configuration
- Local mock data and development-only switches
- Deployment target requirements

## Put Config In One Boundary

- Parse and validate config in `src/shared/config` or the repo's existing shared config module.
- Pass API base URLs and auth providers into `src/adapters`.
- Keep feature flags readable from features and pages without coupling them to transport details.
- Keep secrets out of client bundles.

## Do This In Order

1. Define a typed config object for the public values the client needs.
2. Validate required values at app startup.
3. Pass config into adapters and shared helpers instead of reading env values ad hoc.
4. Keep local mocks explicit and easy to disable.
5. Name and document reconnect intervals, stale thresholds, or polling settings centrally when the app is event-driven.

## Copy This Shape

```ts
export const appConfig = {
  apiBaseUrl: requireEnv("VITE_API_BASE_URL"),
  enableMocks: import.meta.env.VITE_ENABLE_MOCKS === "true",
  streamReconnectMs: Number(import.meta.env.VITE_STREAM_RECONNECT_MS ?? 3000),
};
```

## Never Do These

- Reading env variables directly in many components.
- Shipping server secrets to the browser.
- Hidden dev mocks that change production behavior.
- Feature flags that fork route structure unpredictably.

## Validate

- Missing required config fails early with a useful message.
- API adapters receive config through one boundary.
- Local, staging, and production config paths are obvious.
- Tests can override config deterministically.

## Done When

- Public config is typed and centralized.
- Env reads no longer leak through UI files.
- Event-driven timing or fallback config is explicit where relevant.
- Secret and public config boundaries are clear.

## Handoff

- Use `askr-api-integration` when config shapes adapter behavior.
- Use `askr-realtime-streaming` when reconnect or polling config is the hard part.
- Use `askr-testing-determinism` when config override behavior needs validation.
