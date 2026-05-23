---
name: askr-ssr-ssg
description: Use when building askr server-rendered or static-generated apps, working with SSR/SSG entrypoints, route manifests, static generation configs, hydration-safe rendering, resource constraints, and render output verification.
---

# Askr SSR SSG

Use this when the app renders outside the browser or produces static output. The goal is one shared route tree, environment-safe render paths, and hydration-safe output.

## Inspect First

- `src/main.tsx`
- The shared route registry
- SSR entry files such as `server.ts` or `entry-server.tsx`
- SSG files such as `ssg.config.ts` or `ssg-build.ts`
- Existing build and preview scripts in `package.json`

## Use This When

- You are adding SSR or SSG routes.
- You need parameterized static entries.
- You need to keep browser-only code out of server render paths.
- You need to verify hydration safety after changing render behavior.

## Do This In Order

1. Keep the route tree shared across browser, server, and static output where possible.
2. Register routes at module load before boot or render.
3. Keep render paths deterministic and environment-safe.
4. Use route `entries` or the template's static config for parameterized SSG paths.
5. Build and preview after changes that cross the render boundary.

## Copy This Shape

```ts
registerRoutes(() => {
  page("/docs/{slug}", DocsPage, {
    entries: async () => [{ slug: "getting-started" }, { slug: "routing" }],
  });
});
```

## Never Do These

- Async components in render paths.
- Route registration that depends on request-time mutation.
- Browser-only globals in server or static render paths without guards.
- Divergent route trees for client, server, and static output.
- Random values or time reads that change initial markup across server and client.

## Static Generation Pattern

```bash
npx @askrjs/cli create ssg my-site
npm run build
npx @askrjs/cli ssg --config ./ssg.config.ts --output ./dist/static
```

## Validate

- SSR or SSG build output is deterministic for the same inputs.
- Hydration has no structural mismatch warnings.
- Generated static routes cover expected params.
- Browser preview still navigates correctly after hydration.

## Done When

- The same route tree still owns navigation across environments.
- Server and static render paths are environment-safe.
- Parameterized routes have explicit static coverage where needed.
- Build and preview prove the render boundary still works.

## Handoff

- Use `askr-routing-layouts` when the hard part is route ownership.
- Use `askr-testing-determinism` when hydration, preview, or build verification is the real blocker.
