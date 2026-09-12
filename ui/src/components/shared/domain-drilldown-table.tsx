import { state } from "@askrjs/askr";
import { Link } from "@askrjs/askr/router";
import { ArrowDownIcon, ArrowUpDownIcon, ArrowUpIcon, SearchIcon, XIcon } from "@askrjs/lucide";
import { Input } from "@askrjs/ui";
import { Button, Text } from "@askrjs/themes/components";
import DataTable, { type DataTableColumn } from "./data-table";
import { QueryCompactEmptyState } from "./query-state";
import { formatNumber } from "@/shared/format";

export type DomainDrilldownSortDirection = "asc" | "desc";

export interface DomainDrilldownSortState {
  columnId: string;
  direction: DomainDrilldownSortDirection;
}

export interface DomainDrilldownMetricColumn<Row> {
  cell: (row: Row) => unknown;
  header: string;
  id: string;
  sortValue?: (row: Row) => number | null | undefined;
  title?: (row: Row) => string | undefined;
  width?: string;
}

export interface DomainDrilldownTableProps<Row> {
  description?: string;
  emptyDescription: string;
  id: string;
  metricColumns?: readonly DomainDrilldownMetricColumn<Row>[];
  onRowOpen: (row: Row) => void;
  onSearchChange?: (value: string) => void;
  primaryHeader: string;
  primaryText: (row: Row) => string;
  primaryWidth?: string;
  rowHref: (row: Row) => string;
  rowKey: (row: Row) => string;
  rows: readonly Row[];
  searchLabel?: string;
  searchText?: (row: Row) => string;
  searchValue?: string;
  title: string;
}

export function DomainDrilldownMetricText(props: { children: unknown; title?: string }) {
  return (
    <Text
      as="span"
      class="domain-resource-metric"
      font="mono"
      numeric="tabular"
      size="sm"
      title={props.title}
      weight="medium"
    >
      {props.children}
    </Text>
  );
}

export function filterDrilldownRows<Row>(
  rows: readonly Row[],
  searchValue: string,
  searchText: (row: Row) => string,
) {
  const filter = searchValue.trim().toLowerCase();

  return filter.length === 0
    ? [...rows]
    : rows.filter((row) => searchText(row).toLowerCase().includes(filter));
}

function metricSortValue<Row>(column: DomainDrilldownMetricColumn<Row>, row: Row) {
  const value = column.sortValue?.(row);

  return value === undefined || value === null || !Number.isFinite(value) ? null : value;
}

export function sortDrilldownRows<Row>(
  rows: readonly Row[],
  metricColumns: readonly DomainDrilldownMetricColumn<Row>[],
  sort: DomainDrilldownSortState | null,
) {
  if (!sort) {
    return [...rows];
  }

  const column = metricColumns.find(
    (metricColumn) => metricColumn.id === sort.columnId && metricColumn.sortValue,
  );

  if (!column) {
    return [...rows];
  }

  return rows
    .map((row, index) => ({ index, row, value: metricSortValue(column, row) }))
    .sort((first, second) => {
      if (first.value === null && second.value === null) {
        return first.index - second.index;
      }

      if (first.value === null) {
        return 1;
      }

      if (second.value === null) {
        return -1;
      }

      const comparison = first.value - second.value;
      return sort.direction === "asc" ? comparison : -comparison;
    })
    .map((entry) => entry.row);
}

function shouldIgnoreRowClick(event: MouseEvent) {
  if (event.defaultPrevented) return true;
  const target = event.target;
  return target instanceof Element && target.closest("a,button,input,select,textarea") !== null;
}

export default function DomainDrilldownTable<Row>({
  description,
  emptyDescription,
  id,
  metricColumns = [],
  onRowOpen,
  onSearchChange,
  primaryHeader,
  primaryText,
  primaryWidth,
  rowHref,
  rowKey,
  rows: allRows,
  searchLabel,
  searchText,
  searchValue = "",
  title,
}: DomainDrilldownTableProps<Row>) {
  const [sortState, setSortState] = state<DomainDrilldownSortState | null>(null);
  const currentSort = sortState();
  const searchable = Boolean(onSearchChange);
  const rowText = searchText ?? primaryText;
  const routeFilter = searchValue.trim().toLowerCase();
  const filteredRows = searchable
    ? filterDrilldownRows(allRows, searchValue, rowText)
    : [...allRows];
  const rows = sortDrilldownRows(filteredRows, metricColumns, currentSort);
  const hasMetrics = metricColumns.length > 0;
  const searchInputId = `${id}-search`;
  const tableId = `${id}-table`;

  function sortHeader(column: DomainDrilldownMetricColumn<Row>) {
    if (!column.sortValue) {
      return column.header;
    }

    const active = currentSort?.columnId === column.id;
    const nextDirection: DomainDrilldownSortDirection =
      active && currentSort.direction === "desc" ? "asc" : "desc";
    const directionLabel = active
      ? currentSort.direction === "desc"
        ? "descending"
        : "ascending"
      : "not sorted";
    const SortIcon = active
      ? currentSort.direction === "desc"
        ? ArrowDownIcon
        : ArrowUpIcon
      : ArrowUpDownIcon;

    return (
      <button
        type="button"
        class="domain-sort-button"
        aria-label={`Sort by ${column.header}, ${directionLabel}`}
        title={`Sort by ${column.header}`}
        onClick={() => setSortState({ columnId: column.id, direction: nextDirection })}
      >
        <span>{column.header}</span>
        <SortIcon class="domain-sort-indicator" size={14} aria-hidden="true" />
      </button>
    );
  }

  const columns: readonly DataTableColumn<Row>[] = [
    {
      id: "route",
      header: primaryHeader,
      width: primaryWidth ?? (hasMetrics ? "28%" : "100%"),
      cellComponent: ({ row }) => {
        const label = primaryText(row);

        return (
          <Link class="domain-link-cell" href={rowHref(row)} title={label}>
            {label}
          </Link>
        );
      },
    },
    ...metricColumns.map((column): DataTableColumn<Row> => ({
      id: column.id,
      header: sortHeader(column),
      width: column.width,
      cellComponent: ({ row }) => (
        <DomainDrilldownMetricText title={column.title?.(row)}>
          {column.cell(row)}
        </DomainDrilldownMetricText>
      ),
    })),
  ];

  return (
    <section class="domain-section domain-resource-inventory" aria-labelledby={id}>
      <div class="domain-section-header">
        <div>
          <h2 id={id}>{title}</h2>
          {description ? <p>{description}</p> : null}
        </div>
        <span role="status" aria-live="polite" aria-atomic="true">
          {routeFilter
            ? `${formatNumber(filteredRows.length)} of ${formatNumber(allRows.length)} visible`
            : `${formatNumber(allRows.length)} visible`}
        </span>
      </div>

      {searchable ? (
        <div class="domain-inventory-toolbar">
          <div class="domain-inventory-search-shell">
            <SearchIcon class="domain-inventory-search-icon" size={16} aria-hidden="true" />
            <Input
              id={searchInputId}
              aria-label={searchLabel ?? `Search ${title}`}
              aria-controls={tableId}
              class="domain-inventory-search"
              type="search"
              value={searchValue}
              onInput={(event: Event) => onSearchChange?.((event.target as HTMLInputElement).value)}
              placeholder="Search routes"
            />
          </div>
          {routeFilter ? (
            <Button
              variant="ghost"
              onPress={() => {
                onSearchChange?.("");
                queueMicrotask(() => document.getElementById(searchInputId)?.focus());
              }}
            >
              <XIcon size={16} aria-hidden="true" />
              Clear search
            </Button>
          ) : null}
        </div>
      ) : null}

      {allRows.length === 0 ? (
        <QueryCompactEmptyState description={emptyDescription} />
      ) : rows.length === 0 ? (
        <QueryCompactEmptyState description="No routes match the current search. Clear filters to show all routes." />
      ) : (
        <>
          {hasMetrics ? (
            <p class="domain-inventory-scroll-hint">Scroll horizontally to view every metric.</p>
          ) : null}
          <DataTable<Row>
            id={tableId}
            ariaLabel={title}
            class="domain-resource-data-table"
            dataHasMetrics={hasMetrics}
            columns={columns}
            getKey={(row) => rowKey(row)}
            onRowClick={(row, _rowIndex, _rowKey, event) => {
              if (!shouldIgnoreRowClick(event)) {
                onRowOpen(row);
              }
            }}
            rows={rows}
          />
        </>
      )}
    </section>
  );
}
