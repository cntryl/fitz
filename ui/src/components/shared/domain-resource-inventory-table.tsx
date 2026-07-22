import { state } from "@askrjs/askr";
import { Link, currentRoute, navigate, updateRouteQuery } from "@askrjs/askr/router";
import { ArrowDownIcon, ArrowUpDownIcon, ArrowUpIcon, SearchIcon, XIcon } from "@askrjs/lucide";
import { Input, VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import { Button, Text } from "@askrjs/themes/components";
import { QueryEmptyState } from "./query-state";
import type { ResourceInventoryResource } from "@/features/resource/resource-models";
import { formatNumber } from "@/shared/format";
import { domainScopeHref, formatFitzRoute, type DomainSegment } from "@/shared/navigation/domains";

export interface DomainResourceInventoryArea {
  area: string;
  resourceEntries?: ResourceInventoryResource[];
  resources: string[];
}

export interface DomainResourceInventoryRealm {
  areas: DomainResourceInventoryArea[];
  realm: string;
}

export interface DomainResourceInventory {
  realms: DomainResourceInventoryRealm[];
}

export interface DomainResourceInventoryRow extends ResourceInventoryResource {
  area: string;
  realm: string;
}

export interface DomainResourceMetricColumn {
  cell: (row: DomainResourceInventoryRow) => unknown;
  header: string;
  id: string;
  sortValue?: (row: DomainResourceInventoryRow) => number | null | undefined;
  title?: (row: DomainResourceInventoryRow) => string | undefined;
  width?: string;
}

const ROUTE_SEARCH_QUERY_PARAM = "routeSearch";

export type DomainResourceSortDirection = "asc" | "desc";

export interface DomainResourceInventorySortState {
  columnId: string;
  direction: DomainResourceSortDirection;
}

export interface DomainResourceInventoryTableProps {
  domain: DomainSegment;
  emptyDescription: string;
  inventory?: DomainResourceInventory | null;
  metricColumns?: readonly DomainResourceMetricColumn[];
  routeSearchParam?: string;
  scope?: DomainResourceInventoryScope;
  title: string;
}

export interface DomainResourceInventoryScope {
  area?: string;
  realm?: string;
}

export interface PureDomainResourceInventoryTableProps {
  domain: DomainSegment;
  emptyDescription: string;
  metricColumns?: readonly DomainResourceMetricColumn[];
  onRowOpen: (row: DomainResourceInventoryRow) => void;
  onSearchChange: (value: string) => void;
  rowHref: (row: DomainResourceInventoryRow) => string;
  rows: DomainResourceInventoryRow[];
  searchValue: string;
  title: string;
}

export function domainResourceInventoryRows(
  inventory: DomainResourceInventory | null | undefined,
): DomainResourceInventoryRow[] {
  return (
    inventory?.realms.flatMap((realm) =>
      realm.areas.flatMap((area) => {
        const entries = area.resourceEntries;
        const resourceEntries =
          entries && entries.length > 0
            ? entries
            : area.resources.map((resource) => ({ resource }));

        return resourceEntries.map((resource) => ({
          area: area.area,
          realm: realm.realm,
          ...resource,
        }));
      }),
    ) ?? []
  );
}

export function scopeDomainResourceInventoryRows(
  rows: readonly DomainResourceInventoryRow[],
  scope: DomainResourceInventoryScope,
) {
  return rows.filter(
    (row) =>
      (scope.realm === undefined || row.realm === scope.realm) &&
      (scope.area === undefined || row.area === scope.area),
  );
}

export function DomainResourceMetricText(props: { children: unknown; title?: string }) {
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

function tableHeight(rowCount: number) {
  return `${Math.min(620, Math.max(140, 44 + rowCount * 48))}px`;
}

function shouldIgnoreRowClick(event: MouseEvent) {
  if (event.defaultPrevented) return true;
  const target = event.target;
  return target instanceof Element && target.closest("a,button,input,select,textarea") !== null;
}

function setRouteSearchQuery(routeSearchParam: string, value: string) {
  updateRouteQuery(
    (params) => {
      if (value.trim().length === 0) {
        params.delete(routeSearchParam);
      } else {
        params.set(routeSearchParam, value);
      }
    },
    { history: "replace" },
  );
}

function routeSearchText(domain: DomainSegment, row: DomainResourceInventoryRow) {
  return [formatFitzRoute(domain, row), row.realm, row.area, row.resource, row.operation]
    .filter((part): part is string => typeof part === "string" && part.length > 0)
    .join(" ")
    .toLowerCase();
}

export function filterDomainResourceInventoryRows(
  domain: DomainSegment,
  rows: DomainResourceInventoryRow[],
  searchValue: string,
) {
  const routeFilter = searchValue.trim().toLowerCase();

  return routeFilter.length === 0
    ? rows
    : rows.filter((row) => routeSearchText(domain, row).includes(routeFilter));
}

function metricSortValue(column: DomainResourceMetricColumn, row: DomainResourceInventoryRow) {
  const value = column.sortValue?.(row);

  return value === undefined || value === null || !Number.isFinite(value) ? null : value;
}

export function sortDomainResourceInventoryRows(
  rows: readonly DomainResourceInventoryRow[],
  metricColumns: readonly DomainResourceMetricColumn[],
  sort: DomainResourceInventorySortState | null,
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

export function PureDomainResourceInventoryTable({
  domain,
  emptyDescription,
  metricColumns = [],
  onRowOpen,
  onSearchChange,
  rowHref,
  rows: allRows,
  searchValue,
  title,
}: PureDomainResourceInventoryTableProps) {
  const [sortState, setSortState] = state<DomainResourceInventorySortState | null>(null);
  const currentSort = sortState();
  const routeFilter = searchValue.trim().toLowerCase();
  const filteredRows = filterDomainResourceInventoryRows(domain, allRows, searchValue);
  const rows = sortDomainResourceInventoryRows(filteredRows, metricColumns, currentSort);
  const hasMetrics = metricColumns.length > 0;

  function sortHeader(column: DomainResourceMetricColumn) {
    if (!column.sortValue) {
      return column.header;
    }

    const active = currentSort?.columnId === column.id;
    const nextDirection: DomainResourceSortDirection =
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

  const columns: readonly VirtualTableColumn<DomainResourceInventoryRow>[] = [
    {
      id: "route",
      header: "Route",
      width: hasMetrics ? "34%" : "100%",
      cellComponent: ({ row }) => {
        const route = formatFitzRoute(domain, row);

        return (
          <Link class="domain-link-cell" href={rowHref(row)} title={route}>
            {route}
          </Link>
        );
      },
    },
    ...metricColumns.map(
      (column): VirtualTableColumn<DomainResourceInventoryRow> => ({
        id: column.id,
        header: sortHeader(column),
        width: column.width,
        cellComponent: ({ row }) => (
          <DomainResourceMetricText title={column.title?.(row)}>
            {column.cell(row)}
          </DomainResourceMetricText>
        ),
      }),
    ),
  ];

  return (
    <section
      class="domain-section domain-resource-inventory"
      aria-labelledby={`${domain}-inventory`}
    >
      <div class="domain-section-header">
        <div>
          <h2 id={`${domain}-inventory`}>{title}</h2>
        </div>
        <span role="status" aria-live="polite" aria-atomic="true">
          {routeFilter
            ? `${formatNumber(filteredRows.length)} of ${formatNumber(allRows.length)} visible`
            : `${formatNumber(allRows.length)} visible`}
        </span>
      </div>

      <div class="domain-inventory-toolbar">
        <div class="domain-inventory-search-shell">
          <SearchIcon class="domain-inventory-search-icon" size={16} aria-hidden="true" />
          <Input
            id={`${domain}-inventory-search`}
            aria-label={`Search ${title}`}
            aria-controls={`${domain}-inventory-table`}
            class="domain-inventory-search"
            type="search"
            value={searchValue}
            onInput={(event: Event) => onSearchChange((event.target as HTMLInputElement).value)}
            placeholder="Search routes"
          />
        </div>
        {routeFilter ? (
          <Button
            variant="ghost"
            onPress={() => {
              onSearchChange("");
              queueMicrotask(() => document.getElementById(`${domain}-inventory-search`)?.focus());
            }}
          >
            <XIcon size={16} aria-hidden="true" />
            Clear search
          </Button>
        ) : null}
      </div>

      {allRows.length === 0 ? (
        <QueryEmptyState description={emptyDescription} />
      ) : rows.length === 0 ? (
        <QueryEmptyState description="No resource routes match the current search. Clear filters to show all routes." />
      ) : (
        <VirtualTable<DomainResourceInventoryRow>
          id={`${domain}-inventory-table`}
          aria-label={title}
          class="domain-resource-virtual-table"
          data-has-metrics={hasMetrics ? "true" : "false"}
          columns={columns}
          getKey={(row) => `${row.realm}:${row.area}:${row.resource}:${row.operation ?? ""}`}
          headerHeight={44}
          onRowClick={(row, _rowIndex, _rowKey, event) => {
            if (!shouldIgnoreRowClick(event)) {
              onRowOpen(row);
            }
          }}
          overscan={8}
          rowHeight={48}
          rows={rows}
          style={{ height: tableHeight(rows.length) }}
        />
      )}
    </section>
  );
}

export default function DomainResourceInventoryTable({
  domain,
  emptyDescription,
  inventory,
  metricColumns = [],
  routeSearchParam = ROUTE_SEARCH_QUERY_PARAM,
  scope,
  title,
}: DomainResourceInventoryTableProps) {
  const route = currentRoute();
  const searchQuery = route.query.get(routeSearchParam) ?? "";
  const routeScope = scope ?? {
    area: decodeRouteParam(route.params.area),
    realm: decodeRouteParam(route.params.realm),
  };
  const rows = scopeDomainResourceInventoryRows(domainResourceInventoryRows(inventory), routeScope);

  return (
    <PureDomainResourceInventoryTable
      domain={domain}
      emptyDescription={emptyDescription}
      metricColumns={metricColumns}
      onRowOpen={(row) => navigate(domainScopeHref(domain, row))}
      onSearchChange={(value) => setRouteSearchQuery(routeSearchParam, value)}
      rowHref={(row) => domainScopeHref(domain, row)}
      rows={rows}
      searchValue={searchQuery}
      title={title}
    />
  );
}

function decodeRouteParam(value: string | undefined) {
  if (!value) return undefined;

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}
