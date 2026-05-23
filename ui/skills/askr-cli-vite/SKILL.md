---
name: askr-cli-vite
description: Use when scaffolding Askr projects with @askrjs/cli, choosing spa/ssr/ssg/startkit templates, configuring @askrjs/vite, JSX import source, Vite build setup, generated app customization, or fixing transform wiring.
---

# Askr CLI Vite

Use this only when the task is scaffold choice, initial project setup, or repair of Vite and transform wiring. It is not a normal feature-work skill after the app is already on the canonical path.

## Use This When

- Choosing between `spa`, `ssr`, `ssg`, and `startkit`.
- Fixing `@askrjs/vite` plugin wiring or JSX import-source setup.
- Repairing generated `package.json`, `vite.config.ts`, or `tsconfig` settings.
- Adjusting scaffolded build integration without changing runtime architecture.

## Inspect First

- `docs/create.md`
- `docs/workflows.md`
- Existing `package.json`, `vite.config.ts`, and `tsconfig.json`
- The nearest matching template under `templates/`

## Start From The Closest Template

- `startkit`: default for new product apps with dashboard, accounts, settings, login, themes, icons, and common checks.
- `spa`: minimal client-rendered interactive app.
- `ssr`: server-rendered app boundary.
- `ssg`: static generation scaffold with `ssg.config.ts`.

## Do This In Order

1. Choose the closest template instead of starting from raw Vite.
2. Preserve generated Vite wiring unless the app has a concrete build requirement.
3. Keep `askr()` as the owning plugin for Askr JSX and transforms.
4. Keep runtime route, data, and component decisions out of build config.
5. Treat generated files as app-owned after scaffold, not immutable.
6. Validate scripts and transforms before moving on to feature work.

## Copy This Shape

```ts
import { defineConfig } from "vite";
import { askr } from "@askrjs/vite";

export default defineConfig({
  plugins: [askr()],
});
```

## Never Do These

- Duplicating JSX transform setup in Vite, `tsconfig`, and custom esbuild config.
- Choosing `startkit` for a tiny isolated demo when `spa` fits better.
- Treating CLI-generated files as immutable.
- Adding runtime route or data decisions to build config.

## Validate

- `vite.config.ts` uses `askr()`.
- `package.json` scripts match the selected template.
- `tsconfig` JSX settings match template conventions.
- `npm run dev`, `npm run build`, and available checks pass after setup.

## Done When

- The template matches the runtime boundary the app actually needs.
- Vite wiring stays package-owned and minimal.
- Generated files are ready for normal workflow skills.
- No runtime architecture leaked into build config.

## Handoff

- Use `askr-app-builder` only when the task is still a broad app brief.
- Use `askr-ssr-ssg` when the render boundary is the hard part.
- Use the normal route, data, or UI workflow skills once the scaffold exists.
