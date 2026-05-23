---
name: askr-testing-determinism
description: Use when testing Askr apps, validating deterministic runtime behavior, writing jsdom or browser tests, checking accessibility, cleanup, route behavior, resources, queries, mutations, keyed lists, and performance-sensitive UI.
---

# Askr Testing Determinism

Use this when choosing or running validation for Askr work. The goal is the narrowest executable check that proves the changed contract.

## Use This When

- You changed route behavior, state behavior, async ownership, or user-visible UI.
- You need to decide which test level is correct.
- You need objective done criteria before closing a task.
- You need to avoid ending on diff review alone.

## Inspect First

- Existing tests nearest to the changed files.
- The repo's test scripts in `package.json`.
- Existing browser or jsdom coverage for similar behavior.
- Any known failing test that already describes the bug.

## Pick The Smallest Useful Check

- Unit tests: pure helpers, formatters, validators, route metadata, data transforms.
- jsdom tests: runtime state, resource behavior, route rendering, component logic.
- Browser tests: focus, keyboard behavior, overlays, layout, hydration, visual regressions, performance-sensitive flows.
- Type tests: public API and component prop contracts.
- Benchmarks: hot paths, keyed lists, large tables, router matching, hydration.

## Validate In This Order

1. The narrowest failing or behavior-scoped test for the slice you changed.
2. The nearest type check or package check.
3. Build or template-specific checks when boot, routing, SSR, SSG, or packaging changed.
4. Manual runtime verification for user-visible async, focus, overflow, or stale-state behavior when tests do not already cover it.

## Determinism Targets

- Event ordering and batched updates.
- Stable call order for runtime helpers.
- Keyed DOM identity during list updates.
- No partial DOM commit after render failure.
- Async cancellation on navigation or unmount.
- Route identity and layout retention across navigation.

## Patterns

- Clean up mounted apps with `cleanupApp(root)` when tests mount an app.
- Prefer user-visible assertions over implementation detail checks.
- For `resource()`, cover pending, success, error, refresh, and cancellation when relevant.
- For `@askrjs/ui`, cover keyboard and ARIA behavior.
- For themes, include computed style and overflow checks where visual contracts matter.

- Tests that depend on uncontrolled timers or random data.
- Snapshot-only coverage for interactive behavior.
- Browser behavior tested only in jsdom.
- Performance changes without a focused benchmark or regression test.

## Validate The Test Itself

- The smallest meaningful test covers the changed contract.
- The chosen test level matches the behavior under change.
- Async checks wait for visible state instead of sleeping blindly.
- The repo's existing scripts are used before inventing new ones.

## Done When

- Diff review was not used as the only proof when an executable check existed.

## Handoff

- Use `askr-accessibility` when browser coverage reveals focus or announcement issues.
- Use `askr-observability-debugging` when tests expose stale-state or correlation failures.
