import { navigate } from "@askrjs/askr/router";
import DomainDrilldownTable from "./domain-drilldown-table";
import { areaRollupRows, realmRollupRows, type DomainScopeRow } from "./domain-inventory-rollup";
import type {
  DomainResourceInventory,
  DomainResourceInventoryRow,
  DomainResourceMetricColumn,
} from "./domain-resource-inventory-table";
import { domainScopeHref, formatFitzRoute, type DomainSegment } from "@/shared/navigation/domains";

function joinHeaders(headers: readonly string[]) {
  return headers.length === 1
    ? headers[0]
    : `${headers.slice(0, -1).join(", ")} and ${headers[headers.length - 1]}`;
}

function rollupDescription(columns: readonly DomainResourceMetricColumn[]) {
  const clauses = (["worst", "earliest"] as const).flatMap((rollup) => {
    const headers = columns
      .filter((column) => column.rollup === rollup)
      .map((column) => column.header);
    if (headers.length === 0) return [];
    const verb = headers.length === 1 ? "shows" : "show";
    const what = rollup === "worst" ? "the worst single resource" : "the soonest single resource";
    return [`${joinHeaders(headers)} ${verb} ${what}`];
  });
  const summed = "Counts and rates are summed across each row's resources";

  return clauses.length === 0 ? `${summed}.` : `${summed}; ${clauses.join("; ")}.`;
}

export interface DomainScopeInventoryTableProps {
  domain: DomainSegment;
  emptyDescription: string;
  metricColumns?: readonly DomainResourceMetricColumn[];
  inventory?: DomainResourceInventory | null;
  onSearchChange: (value: string) => void;
  realm?: string;
  rows: readonly DomainResourceInventoryRow[];
  searchValue: string;
}

export default function DomainScopeInventoryTable({
  domain,
  emptyDescription,
  metricColumns = [],
  inventory,
  onSearchChange,
  realm,
  rows,
  searchValue,
}: DomainScopeInventoryTableProps) {
  const showingAreas = realm !== undefined;
  const realmInventory = inventory?.realms.find((entry) => entry.realm === realm);
  const scopeRows = showingAreas
    ? areaRollupRows(rows, realmInventory?.areas, realm)
    : realmRollupRows(rows, inventory?.realms);
  const scopeHref = (row: DomainScopeRow) =>
    domainScopeHref(domain, { area: row.area, realm: row.realm });

  return (
    <DomainDrilldownTable<DomainScopeRow>
      description={metricColumns.length > 0 ? rollupDescription(metricColumns) : undefined}
      emptyDescription={emptyDescription}
      id={`${domain}-inventory`}
      metricColumns={metricColumns}
      onRowOpen={(row) => navigate(scopeHref(row))}
      onSearchChange={onSearchChange}
      primaryHeader="Route"
      primaryText={(row) => formatFitzRoute(domain, { area: row.area, realm: row.realm })}
      rowHref={scopeHref}
      rowKey={(row) => `${row.realm}:${row.area ?? ""}`}
      rows={scopeRows}
      searchLabel={showingAreas ? "Search areas" : "Search realms"}
      searchValue={searchValue}
      title={showingAreas ? "Areas" : "Realms"}
    />
  );
}
