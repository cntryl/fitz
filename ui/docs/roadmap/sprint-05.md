# Sprint 05: Metrics Explorer

## Objective

Make raw Prometheus metrics usable without turning the page into a wall of text.

## Routes and Files

- `/admin/metrics`
- `ui/src/features/metrics/metrics-page.tsx`
- `ui/src/features/metrics/metrics-mappers.ts`
- `ui/src/styles/forms.css`
- `ui/src/styles/domain.css`

## Requirements

- Page explains whether metrics loaded and how many families are visible.
- Search/filter is prominent but not oversized.
- Summary groups are useful shortcuts, not duplicated dashboard cards.
- Metric family rows are compact and easy to scan.
- Raw sample values preserve monospace readability.
- Empty filter state offers a clear reset path.
- Error state distinguishes metrics endpoint failure from global broker failure.

## Deliverables

- Metrics header, filter toolbar, summary groups, and family list visually normalized.
- Long metric names and label sets wrap or truncate intentionally.
- Mobile view reviewed for table/card density.
- Dark mode reviewed for code/monospace contrast.

## Acceptance Criteria

- A user can find a metric family from the first viewport.
- Long metric names do not force horizontal page overflow.
- Filtered empty state does not look like a broker-empty state.
- Summary group labels are shorter than the sample details they summarize.
- Metric samples remain copyable and legible.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/format.test.ts`
- Screenshot: metrics loaded, metrics filtered empty, metrics mobile.

## Out Of Scope

- Time-series charts.
- Metrics query language.
- Export/download features.
