# askr-themes bug list

## Open

### Inline layout does not receive gap and wrap styling

- Status: local workaround applied
- Affected local files:
  - `src/pages/app/sessions.tsx`
  - `src/pages/app/queue.tsx`
  - `src/pages/app/queue-resource.tsx`
- Symptom:
  - `<Inline gap="3" wrap="wrap">` renders without the expected spacing between children.
- Root cause:
  - `Inline` renders with `data-slot="inline"`, but the default theme CSS gap and wrap selectors target `[data-slot="flex"]`.
- Expected:
  - `Inline` should receive the same layout token handling as `Flex` for `gap`, `gapX`, `gapY`, and `wrap`.
- Local workaround:
  - Use `Flex` in action rows until the theme selectors include `inline`.

### NavGroup align=end is not reliable in sidebar layouts

- Status: observed, no local patch yet
- Affected local file:
  - `src/pages/app/_layout.tsx`
- Symptom:
  - `<NavGroup align="end">` does not consistently read as "pinned to the bottom" of the sidebar.
- Notes:
  - The component emits `data-align="end"` correctly.
  - The sidebar theme includes a rule that applies `margin-block-start: auto` to end-aligned groups.
  - In stacked or mobile sidebar layouts, the shell nav is not stretched to a full viewport-height column, so there may be no spare block space for the auto margin to consume.
- Expected:
  - `align="end"` should either behave consistently across sidebar modes, or the docs should make the viewport/layout dependency explicit.
