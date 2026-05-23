---
name: askr-design-system
description: Use when creating product-grade Askr interfaces, design-system rules, density, page hierarchy, navigation, forms, tables, panels, action placement, tokens, dark mode, responsive behavior, and operational UI polish.
---

# Askr Design System

Use this only when a repeated product pattern must become shared across multiple screens. This is not the default starting skill for a single UI task.

## Inspect First

- Existing `src/styles`, tokens, theme imports, and shared components.
- `src/components/shared` before adding new UI building blocks.
- `@askrjs/themes` primitives already in use.
- Target product audience and primary workflows.

## Use This When

- The same pattern repeats across three or more screens.
- Theme primitives alone are not enough to express a product-specific shared pattern.
- You need consistent density, hierarchy, and action placement across the app.
- You are about to create a reusable product component, not just style one page.

## Product UI Defaults

- Prefer quiet, dense, scannable SaaS/admin interfaces.
- Use page headers, toolbars, panels, forms, tables, and clear action hierarchy.
- Keep primary actions close to the object or workflow they affect.
- Use icons for familiar tool actions and text for consequential commands.
- Use tokens and semantic slots rather than one-off visual styles.
- Before inventing a component, check `@askrjs/themes` layout, control, surface, feedback, shell, nav, and overlay exports.

## Do This In Order

1. Prove the pattern is repeated enough to deserve extraction.
2. Start from existing theme primitives and shared styles.
3. Extract the smallest product-specific shared component or CSS recipe that removes repetition.
4. Keep tokens in CSS and keep workflow logic out of design-system components.
5. Validate the pattern in light, dark, responsive, empty, error, and disabled states.

## Start From These Primitives

- Page width/rhythm: `Container`, `Section`.
- Vertical and horizontal composition: `Stack`, `Inline`, `Flex`.
- Responsive groups: `Block`.
- Low-level layout: `Box`, `Spacer`, `AspectRatio`.
- Product surfaces: `Card`, `Alert`, `Badge`, `ListGroup`, `Separator`, `Skeleton`.
- Forms/actions: `Button`, `ButtonGroup`, `Close`, `Field`, `FieldHint`, `FieldError`, `InputGroup`.
- Async states: `EmptyState`, `Spinner`, plus query/resource state-specific copy.
- App chrome: `Shell`, `ShellNav`, `ShellMain`, `Header`, `Sidebar`, `Navbar`.
- Navigation: `Nav`, `NavLink`, `NavGroup`, `NavBrand`, `Breadcrumb`, `Pagination`.
- Menus: themed `Dropdown`, `Menu`, and `Menubar` from `@askrjs/themes/overlays`.

## Never Do These

- Route layouts own shell chrome.
- Shared components own repeatable product surfaces.
- Page sections should be unframed layout regions; use cards/panels for repeated items or contained tools.
- Do not wrap cards inside cards; use `Section`, `Container`, `Stack`, and `Block` for page-level composition.
- Tables and forms should be compact but readable.
- Long labels must wrap or truncate intentionally.
- Marketing-page composition for operational apps.
- Decorative gradients, oversized hero panels, or low-density cards in work surfaces.
- Hardcoded colors and spacing outside tokens.
- Component variants that duplicate what CSS state or slots should handle.
- App-local design-system clones of theme primitives.
- Custom CSS grids or flex wrappers when `Block`, `Flex`, `Inline`, or `Stack` already express the layout.

## Validate

- Mobile, tablet, desktop, light, and dark modes are considered.
- Focus, hover, disabled, loading, empty, and error states feel consistent.
- Text does not overflow buttons, cards, navs, tables, or overlays.
- The interface optimizes repeated use, not only first impression.

## Done When

- A repeated product pattern became shared without creating a second component catalog.
- Theme primitives still provide the baseline behavior and styling model.
- The extracted pattern improves coherence across real repeated screens.

## Handoff

- Use `askr-theming` when the next step is token or shell styling.
- Use `askr-ui-composition` when the next step is interactive behavior.
- Use `askr-testing-determinism` before closing broad visual-system changes.
