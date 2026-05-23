---
name: askr-dashboard-charts
description: Use when building askr dashboards, stat cards, activity feeds, product metrics, tables, async feedback, and @askrjs/charts visualizations such as area, bar, line, donut, heatmap, timeline, gauges, and chart chrome.
---

# Askr Dashboard Charts

Use this for product dashboards and metric-heavy screens. The goal is route-owned data, deterministic metric formatting, and charts that answer product questions without inventing parallel UI systems.

## Use This When

- You are building a dashboard, metrics screen, stat-card surface, or chart-heavy route.
- The page needs resource-owned async loading plus chart composition.
- Charts and tables must share the same formatter logic.
- You need loading, empty, and error truth on a dense metrics screen.

## Inspect First

- `templates/startkit/src/pages/workspace/dashboard.tsx`
- Existing stat card, table, empty state, and chart styles
- Existing format helpers in `src/shared`
- Existing route or feature owners for the metrics you need

## Choose The Owner

- The route or page owns high-level loading and composition.
- `src/components/shared` owns reusable stat cards, page headers, tables, and empty states.
- `src/features/<domain>` owns domain-specific dashboard panels and metric workflows.
- `src/shared` owns formatting helpers so cards, tables, and charts stay consistent.

## Do This In Order

1. Load dashboard data in the route or feature container with `resource()` unless the data must be shared across screens.
2. Keep metric formatting in shared helpers.
3. Use `@askrjs/charts` components for chart visuals and keep CSS imported at the app boundary.
4. Pair charts with labels, summaries, or tables when precision matters.
5. Keep loading, empty, error, and refresh states explicit.

## Copy This Shape

```tsx
import { resource } from "@askrjs/askr/resources";
import { AreaChart, ChartPanel, ChartShell } from "@askrjs/charts/components";

const dashboard = resource(({ signal }) => loadDashboard({ signal }), []);

if (dashboard.pending && !dashboard.value) return <p>Loading dashboard...</p>;
if (dashboard.error) return <p role="alert">Unable to load dashboard.</p>;

<ChartShell title="Revenue" description="Last 7 days">
  <ChartPanel title="Trend">
    <AreaChart label="Revenue trend" data={dashboard.value.revenue} />
  </ChartPanel>
</ChartShell>;
```

## Never Do These

- Inline mock metrics inside page JSX.
- Hardcoded chart colors in runtime code.
- Decorative charts that do not answer a product question.
- Dense dashboards without loading, empty, and error states.

## Validate

- Cards and charts share formatter logic.
- The page scans cleanly at mobile, tablet, and desktop widths.
- Chart data has stable labels and keys.
- Loading, empty, error, and refresh states are visible.

## Done When

- Route-owned loading and chart composition are clear.
- Metric formatting is shared and deterministic.
- Charts support the page's product question instead of decorating it.
- Async states remain explicit on the dashboard surface.

## Handoff

- Use `askr-resources-data` when async ownership is the real blocker.
- Use `askr-theming` when the hard part is shell and visual coherence.
- Use `askr-testing-determinism` before closing responsive or stateful chart changes.
