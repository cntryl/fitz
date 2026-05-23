---
name: askr-accessibility
description: Use when making Askr UI accessible, including keyboard flows, focus management, ARIA names, dialogs, menus, forms, tables, live regions, screen-reader behavior, and @askrjs/ui primitive selection.
---

# Askr Accessibility

Use this for accessibility-sensitive UI or when composing interactive primitives. The goal is predictable keyboard flow, visible and announced state changes, and reuse of primitives that already own behavior.

## Use This When

- The surface includes dialogs, menus, forms, tables, or other interactive controls.
- Focus management, keyboard flow, or screen-reader behavior matters to the task.
- Async status, stale state, or destructive confirmation must be announced clearly.
- You need to review whether a UI change stayed accessible after composition or theming work.

## Inspect First

- Existing `@askrjs/ui` primitive usage
- Labels, accessible names, focus order, and keyboard behavior
- Form errors, async status messages, and destructive confirmations
- Browser tests for overlays, menus, forms, and tables

## Do This In Order

1. Prefer `@askrjs/ui` for behavior-heavy controls before raw HTML.
2. Make sure every interactive control has an accessible name.
3. Keep dialog, menu, and overlay focus behavior predictable.
4. Make form errors, async failures, and important status changes visible and announceable.
5. Check that stale, selected, pending, and error states are not color-only.

## Copy This Shape

```tsx
<Show when={mutation.error}>
	<p role="alert">Unable to save changes.</p>
</Show>

<button aria-label="Archive account">Archive</button>
```

## Never Do These

- Reimplementing primitive keyboard behavior.
- Icon-only buttons without labels.
- Focus loss after route changes, dialogs, or list updates.
- Color-only error, stale, selected, or pending states.
- Toast-only critical errors.

## Validate

- Keyboard-only users can complete the workflow.
- Focus lands in the expected place after navigation, dialog open or close, and submit.
- Axe or equivalent accessibility checks pass for changed surfaces when available.
- Browser tests cover complex focus and keyboard behavior.

## Done When

- Keyboard, focus, and naming behavior are predictable.
- Important async and failure states are announced or visible semantically.
- The surface does not depend on color alone to communicate state.
- Accessibility behavior is verified at the right test level.

## Handoff

- Use `askr-ui-composition` when the hard part is primitive behavior composition.
- Use `askr-error-loading-empty` when async state truth is the blocker.
- Use `askr-testing-determinism` before closing browser-level accessibility changes.
