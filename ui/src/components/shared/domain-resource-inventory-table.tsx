import { currentRoute, navigate, updateRouteQuery } from "@askrjs/askr/router";
import DomainDrilldownTable, { type DomainDrilldownMetricColumn } from "./domain-drilldown-table";
import type { DomainMetricRow } from "./domain-inventory-rollup";
import type { ResourceInventoryResource } from "@/features/resource/resource-models";
import {
  domainResourceHref,
  formatFitzRoute,
  type DomainSegment,
} from "@/shared/navigation/domains";

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

/**
 * Columns read only metric fields, so the same definition renders a resource row,
 * a realm rollup, and an area rollup without change.
 */
export type DomainResourceMetricColumn = DomainDrilldownMetricColumn<DomainMetricRow>;

export const ROUTE_SEARCH_QUERY_PARAM = "routeSearch";

export interface DomainResourceInventoryScope {
  area?: string;
  realm?: string;
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

export function decodeRouteParam(value: string | undefined) {
  if (!value) return undefined;

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

export function setRouteSearchQuery(routeSearchParam: string, value: string) {
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

export function routeSearchText(domain: DomainSegment, row: DomainResourceInventoryRow) {
  return [formatFitzRoute(domain, row), row.realm, row.area, row.resource, row.operation]
    .filter((part): part is string => typeof part === "string" && part.length > 0)
    .join(" ");
}

export function PureDomainResourceInventoryTable({
  domain,
  emptyDescription,
  metricColumns = [],
  onRowOpen,
  onSearchChange,
  rowHref,
  rows,
  searchValue,
  title,
}: PureDomainResourceInventoryTableProps) {
  return (
    <DomainDrilldownTable<DomainResourceInventoryRow>
      emptyDescription={emptyDescription}
      id={`${domain}-inventory`}
      metricColumns={metricColumns}
      onRowOpen={onRowOpen}
      onSearchChange={onSearchChange}
      primaryHeader="Route"
      primaryText={(row) => formatFitzRoute(domain, row)}
      rowHref={rowHref}
      rowKey={(row) => `${row.realm}:${row.area}:${row.resource}:${row.operation ?? ""}`}
      rows={rows}
      searchLabel={`Search ${title}`}
      searchText={(row) => routeSearchText(domain, row)}
      searchValue={searchValue}
      title={title}
    />
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
  const resourceHref = (row: DomainResourceInventoryRow) => domainResourceHref(domain, row);

  return (
    <PureDomainResourceInventoryTable
      domain={domain}
      emptyDescription={emptyDescription}
      metricColumns={metricColumns}
      onRowOpen={(row) => navigate(resourceHref(row))}
      onSearchChange={(value) => setRouteSearchQuery(routeSearchParam, value)}
      rowHref={resourceHref}
      rows={rows}
      searchValue={searchQuery}
      title={title}
    />
  );
}
