---
name: askr-theming
description: Use when styling Askr apps with @askrjs/themes, tokens, data-theme, ThemeProvider, shell/nav/layout primitives, data-slot selectors, visual quality audits, dark mode, and theme/app boundary decisions.
---

# Askr Theming

Use this when styling an Askr app with the theme layer. The goal is one obvious shell, one obvious token strategy, and no app-local clones of solved surfaces.

## Inspect First

- `src/styles.css` and `src/styles/*`
- The root layout or app shell that imports the theme
- Existing shared components and `@askrjs/themes` imports
- The nearest screen with the same layout or surface pattern

## Use This When

- You are styling a new product surface.
- You need dark mode, shell chrome, or token overrides.
- You want the default Askr admin or SaaS visual language.
- You are tempted to create local `Card`, `Panel`, `Sidebar`, `Toolbar`, or `EmptyState` components.

## Choose The Layer

- Use `@askrjs/themes` when the existing app already uses the default visual layer.
- Use `@askrjs/ui` directly only when the app already owns its own visual system and just needs headless behavior.
- Create an app-local wrapper only when it adds clear product semantics beyond one screen.

## Do This In Order

1. Import the theme once at the app boundary or stylesheet entry.
2. Reuse existing theme primitives for shells, navigation, layout, surfaces, feedback, and forms.
3. Override semantic `--ak-*` tokens in CSS, not runtime TypeScript.
4. Use `data-slot` or documented selectors for local CSS hooks.
5. Check the result in mobile, desktop, light, dark, empty, error, and disabled states.

## Copy This Shape

```ts
import "@askrjs/themes/default";
import { ThemeProvider } from "@askrjs/themes/theme";
import { Container, Section, Stack } from "@askrjs/themes/layouts";
import { Button } from "@askrjs/themes/controls";
import { Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
import { EmptyState } from "@askrjs/themes/feedback";
import { Shell, ShellMain, ShellNav } from "@askrjs/themes/shells";
```

## Never Do These

- Moving runtime behavior into theme files.
- Treating theme components as app state containers.
- Hardcoded non-token colors when tokens exist.
- Deep internal selectors or `!important`.
- Marketing-page assumptions in operational SaaS surfaces.
- Inventing app-local `Panel`, `HStack`, `VStack`, `Page`, `Toolbar`, `Badge`, `Card`, or `EmptyState` components before checking the theme surface.
- Recreating shell, nav, feedback, form field, or responsive layout primitives already exported by `@askrjs/themes`.

## Validate

- No clipped text, horizontal overflow, or broken shell composition.
- Focus, hover, disabled, empty, and error states are styled deliberately.
- Dark mode is intentional, not inverted by accident.
- App CSS remains override-friendly and token-based.

## Done When

- The surface reuses the existing theme layer instead of inventing a second one.
- Tokens live in CSS and behavior stays in components.
- The interface remains coherent across responsive and state changes.

## Handoff

- Use `askr-ui-composition` when the hard part is behavior and composition, not visual styling.
- Use `askr-accessibility` when announcements, focus, or semantic behavior need review.
- Use `askr-testing-determinism` before closing visible UI changes.
