---
name: askr-ui-composition
description: Use when composing Askr UI with @askrjs/ui headless primitives, accessibility behavior, asChild polymorphism, root-owned state, private component context, slots, keyboard behavior, overlays, and app-local components.
---

# Askr UI Composition

Use this when the hard part is behavior and composition rather than theming. The goal is to reuse `@askrjs/ui` for focus, keyboard, dismissal, and ARIA behavior instead of rebuilding it locally.

## Inspect First

- Existing component imports and slot naming
- The nearest shared component with similar behavior
- Existing theme usage to decide whether headless or themed primitives fit better
- Existing tests for keyboard or focus behavior

## Use This When

- You need dialog, popover, menu, select, toggle, checkbox, or other interactive behavior.
- The app needs headless primitives or already owns its own CSS.
- You are tempted to rebuild keyboard, focus, or dismissal logic locally.
- Themed primitives are not enough because behavior composition is the real problem.

## Choose UI Or Theme

- Use `@askrjs/ui` directly when the app has its own CSS or needs a headless behavior primitive.
- Use `@askrjs/themes` when the existing visual system should also style the primitive.
- Do not create app-local wrappers for solved theme surfaces unless the wrapper adds product semantics.

## Do This In Order

1. Pick the primitive family that already owns the behavior.
2. Keep coordination state in the root component that owns the interaction.
3. Use `asChild` only when caller markup must be preserved.
4. Use `data-slot` on app structure that needs stable styling hooks.
5. Validate keyboard, pointer, and ARIA behavior.

## Copy This Shape

```tsx
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogOverlay,
  AlertDialogPortal,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@askrjs/ui/alert-dialog";

<AlertDialog>
  <AlertDialogTrigger asChild>
    <button>Archive</button>
  </AlertDialogTrigger>
  <AlertDialogPortal>
    <AlertDialogOverlay />
    <AlertDialogContent>
      <AlertDialogTitle>Archive account?</AlertDialogTitle>
      <AlertDialogCancel>Cancel</AlertDialogCancel>
      <AlertDialogAction>Archive</AlertDialogAction>
    </AlertDialogContent>
  </AlertDialogPortal>
</AlertDialog>;
```

## Never Do These

- Reimplementing keyboard, focus, or dismissal behavior already owned by a primitive.
- Styling or business logic inside behavior primitives.
- Leaf components that fetch data or own routing.
- Silent invalid composition when a part requires a root scope.
- Prop bloat where composition would be clearer.

## Validate

- Keyboard and pointer behavior match the primitive contract.
- ARIA labels, names, and roles are present where required.
- `asChild` preserves expected markup and events.
- User-facing interaction has jsdom or browser coverage.

## Done When

- The app reused the primitive that already owns the behavior.
- Product composition stayed in the app, while behavior stayed in the primitive.
- No local behavior clone was introduced.

## Handoff

- Use `askr-theming` when the next step is visual styling and shell coherence.
- Use `askr-accessibility` when the next step is announcement, focus, or semantic review.
- Use `askr-testing-determinism` before closing user-facing interaction changes.
