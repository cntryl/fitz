import { currentRoute, navigate } from "@askrjs/askr/router";
import DomainDrilldownTable, { type DomainDrilldownMetricColumn } from "./domain-drilldown-table";
import { ROUTE_SEARCH_QUERY_PARAM, setRouteSearchQuery } from "./domain-resource-inventory-table";
import { domainScopeHref, formatFitzRoute, type DomainSegment } from "@/shared/navigation/domains";

export interface DomainOperationRow {
  operation: string;
}

export type DomainOperationMetricColumn<Row extends DomainOperationRow> =
  DomainDrilldownMetricColumn<Row>;

export interface DomainOperationScope {
  area: string;
  realm: string;
  resource: string;
}

export interface DomainOperationTableProps<Row extends DomainOperationRow> {
  description?: string;
  domain: DomainSegment;
  emptyDescription: string;
  metricColumns?: readonly DomainOperationMetricColumn<Row>[];
  rows: readonly Row[];
  scope: DomainOperationScope;
  title: string;
}

/**
 * Operation rows are the last drilldown tier and use the same table contract as
 * realms, areas, and resources: full route, row click, search, sortable metrics.
 */
export default function DomainOperationTable<Row extends DomainOperationRow>({
  description,
  domain,
  emptyDescription,
  metricColumns = [],
  rows,
  scope,
  title,
}: DomainOperationTableProps<Row>) {
  const route = currentRoute();
  const searchValue = route.query.get(ROUTE_SEARCH_QUERY_PARAM) ?? "";
  // Some domains report an operation as an already-qualified route; others report
  // the bare operation segment. Both render as one fully-qualified route.
  const routeText = (row: Row) =>
    row.operation.includes("://") ? row.operation : formatFitzRoute(domain, { ...scope, ...row });
  const operationHref = (row: Row) =>
    domainScopeHref(domain, { ...scope, operation: row.operation });

  return (
    <DomainDrilldownTable<Row>
      description={description}
      emptyDescription={emptyDescription}
      id={`${domain}-operations`}
      metricColumns={metricColumns}
      onRowOpen={(row) => navigate(operationHref(row))}
      onSearchChange={(value) => setRouteSearchQuery(ROUTE_SEARCH_QUERY_PARAM, value)}
      primaryHeader="Route"
      primaryText={routeText}
      rowHref={operationHref}
      rowKey={(row) => row.operation}
      rows={rows}
      searchLabel={`Search ${title}`}
      searchValue={searchValue}
      title={title}
    />
  );
}
