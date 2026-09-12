import { navigate } from "@askrjs/askr/router";
import DomainDrilldownTable from "./domain-drilldown-table";
import { areaRollupRows, realmRollupRows, type DomainScopeRow } from "./domain-inventory-rollup";
import type {
  DomainResourceInventory,
  DomainResourceInventoryRow,
  DomainResourceMetricColumn,
} from "./domain-resource-inventory-table";
import { formatNumber } from "@/shared/format";
import { domainScopeHref, formatFitzRoute, type DomainSegment } from "@/shared/navigation/domains";

const ROLLUP_DESCRIPTION =
  "Counts and rates are summed across the scope; latency, age, and next-run columns show the extreme value any one resource reported.";

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

function countColumn(
  id: string,
  header: string,
  value: (row: DomainScopeRow) => number | undefined,
): DomainResourceMetricColumn {
  return {
    id,
    header,
    width: "10%",
    cell: (row) => formatNumber(value(row as DomainScopeRow) ?? 0),
    sortValue: (row) => value(row as DomainScopeRow),
  };
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
  const structuralColumns = showingAreas
    ? [countColumn("resources", "Resources", (row) => row.resourceCount)]
    : [
        countColumn("areas", "Areas", (row) => row.areaCount),
        countColumn("resources", "Resources", (row) => row.resourceCount),
      ];
  const scopeHref = (row: DomainScopeRow) =>
    domainScopeHref(domain, { area: row.area, realm: row.realm });

  return (
    <DomainDrilldownTable<DomainScopeRow>
      description={metricColumns.length > 0 ? ROLLUP_DESCRIPTION : undefined}
      emptyDescription={emptyDescription}
      id={`${domain}-inventory`}
      metricColumns={[...structuralColumns, ...metricColumns]}
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
