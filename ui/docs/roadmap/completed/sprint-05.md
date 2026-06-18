# Sprint 05: Metrics Explorer

## Objective

Make raw Prometheus metrics usable without turning the page into a wall of text.

## Routes and Files

- `/admin/metrics`
- `ui/src/features/metrics/metrics-page.tsx`
- `ui/src/features/metrics/metrics-mappers.ts`
- `ui/src/styles/forms.css`
- `ui/src/styles/domain.css`

## Tasks

1. Metrics header and filter toolbar
   Requirements:
   - The page explains whether metrics loaded and how many families are visible.
   - Search/filter controls are prominent but compact.
   - Filter state remains visible while results update.

   Acceptance Criteria:
   - A user can find the search/filter path from the first viewport.
   - Filter controls align cleanly on desktop and mobile.
   - Pending filter changes do not hide the current result context.

2. Summary shortcuts
   Requirements:
   - Summary groups are useful shortcuts, not duplicated dashboard cards.
   - Labels are short enough to scan faster than raw metric names.
   - Summary state does not imply time-series history.

   Acceptance Criteria:
   - Summary labels are shorter than the sample details they summarize.
   - Each shortcut visibly relates to the filtered family list.
   - Summary groups remain compact in dark and light themes.

3. Metric family and sample rows
   Requirements:
   - Metric family rows are compact and easy to scan.
   - Long metric names, label sets, and sample values wrap or truncate intentionally.
   - Raw sample values preserve monospace readability and remain copyable.

   Acceptance Criteria:
   - Long metric names do not force horizontal page overflow.
   - Metric samples are legible in light and dark mode.
   - Users can distinguish family name, labels, and sample value without decoding layout.

4. Metrics states and coverage
   Requirements:
   - Empty filter state offers a clear reset path.
   - Error state distinguishes metrics endpoint failure from global broker failure.
   - Smoke or format coverage is updated for visible route priorities.

   Acceptance Criteria:
   - Filtered empty state does not look like a broker-empty state.
   - Error copy names the metrics loading failure specifically.
   - Screenshot review covers loaded, filtered empty, mobile, and dark-mode code contrast.

## Validation

- `npm run type-check`
- `npm run lint`
- `npm run test -- tests/page-smoke.test.tsx tests/format.test.ts`
- Screenshot: metrics loaded, metrics filtered empty, metrics mobile.

## Out Of Scope

- Time-series charts.
- Metrics query language.
- Export/download features.
