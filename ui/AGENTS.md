# Fitz UI Agent Guide

## Repo Layout

- `ui/` is the Askr-based admin SPA workspace.
- `skills/` in this directory is the UI-local skill root.
- `../skills/` contains repo-wide skills for shared workflows.
- `../public/openapi.yml` is the UI adapter input; production static assets are served from `/app/public`.

## Read First

- [package.json](package.json)
- [README.md](README.md)
- [src/shared/config.ts](src/shared/config.ts)
- [src/main.tsx](src/main.tsx) when present
- [src/router.tsx](src/router.tsx) or [src/pages/\_routes.tsx](src/pages/_routes.tsx) when present
- The nearest route and layout owners under `src/`
- Existing tests under `tests/`
- [../AGENTS.md](../AGENTS.md) when the task touches shared repo boundaries

## Working Rules

- Keep changes UI-local unless the task explicitly crosses into the broker or shared docs.
- Use the narrowest applicable skill from `skills/` for the slice you are changing.
- Prefer `@askrjs/askr`, `@askrjs/ui`, and `@askrjs/themes` before inventing app-local primitives.
- Regenerate adapters with `npm run gen:adapters` from `ui/` when `../public/openapi.yml` or the client surface changes.
- If a change touches shared repo behavior, consult the root guide and shared skills before editing.

## Validation

- Local setup: `vp install`
- Local run: `vp dev`
- Checks: `npm run type-check`, `npm run test`, `npm run lint`, `npm run build`
- Formatting and generated output: `npm run gen`

## Done When

- The owning UI file or route is clear.
- The relevant local skill in `skills/` was used or intentionally bypassed.
- Validation ran at the narrowest useful scope.
- Shared repo guidance was only consulted when the slice crossed workspace boundaries.
