import { Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import { Alert, Button, Block } from "@askrjs/themes/components";
import DomainHeader from "./domain-header";
import type { DomainHeaderProps } from "./domain-header";
import DomainPageFrame from "./domain-page-frame";
import OperatorScopeStrip from "./operator-scope-strip";
import DomainSummaryStrip from "./domain-summary-strip";
import DomainScopeInventoryTable from "./domain-scope-inventory-table";
import { aggregateDomainMetricRows } from "./domain-inventory-rollup";
import DomainResourceInventoryTable, {
  decodeRouteParam,
  domainResourceInventoryRows,
  ROUTE_SEARCH_QUERY_PARAM,
  scopeDomainResourceInventoryRows,
  setRouteSearchQuery,
  type DomainResourceInventory,
  type DomainResourceMetricColumn,
} from "./domain-resource-inventory-table";
import { QueryErrorState, QueryLoadingState, QueryRefreshingState } from "./query-state";
import { formatUnknownError } from "@/shared/errors/format";
import { formatNumber } from "@/shared/format";
import { domainTitleForSegment, type DomainSegment } from "@/shared/navigation/domains";

export interface DomainInventoryQuery<TInventory extends DomainResourceInventory> {
  data?: TInventory | null;
  error?: unknown;
  loading?: boolean;
  refresh: () => unknown;
  refreshing?: boolean;
  stale?: boolean;
}

export interface DomainInventoryStat {
  caption?: string;
  label: string;
  value: string | number;
}

export interface DomainInventoryPageProps<TInventory extends DomainResourceInventory> {
  description: string;
  domain: DomainSegment;
  emptyDescription: string;
  errorTitle: string;
  eyebrow: string;
  inventory: DomainInventoryQuery<TInventory>;
  loadingDescription: string;
  metricColumns?: readonly DomainResourceMetricColumn[];
  refreshing?: boolean;
  refreshers?: Array<() => unknown>;
  refreshLabel: string;
  refreshingDescription: string;
  stats?: readonly DomainInventoryStat[];
  status?: DomainHeaderProps["status"];
  tableTitle: string;
  title: string;
}

function refreshAll(refreshers: Array<() => unknown>) {
  for (const refresh of refreshers) {
    void refresh();
  }
}

/** Only primitive cells can be summarised; a cell rendering markup is skipped. */
function statValue(value: unknown): string | number | null {
  return typeof value === "number" || typeof value === "string" ? value : null;
}

export default function DomainInventoryPage<TInventory extends DomainResourceInventory>({
  description,
  domain,
  emptyDescription,
  errorTitle,
  eyebrow,
  inventory,
  loadingDescription,
  metricColumns = [],
  refreshing,
  refreshers,
  refreshLabel,
  refreshingDescription,
  stats = [],
  status,
  tableTitle,
  title,
}: DomainInventoryPageProps<TInventory>) {
  const route = currentRoute();
  const realm = decodeRouteParam(route.params.realm);
  const area = decodeRouteParam(route.params.area);
  const searchValue = route.query.get(ROUTE_SEARCH_QUERY_PARAM) ?? "";
  const domainTitle = domainTitleForSegment(domain);
  const allRows = domainResourceInventoryRows(inventory.data);
  const scopedRows = scopeDomainResourceInventoryRows(allRows, { area, realm });
  const scopedRealm = inventory.data?.realms.find((entry) => entry.realm === realm);
  const pageTitle = area ?? realm ?? title;
  const pageEyebrow = area ? `${domainTitle} area` : realm ? `${domainTitle} realm` : eyebrow;
  const pageDescription = area
    ? `Resources in ${realm} / ${area}.`
    : realm
      ? `Areas in the ${realm} realm.`
      : description;
  const onRefresh = () => refreshAll(refreshers ?? [inventory.refresh]);
  const isRefreshing = refreshing ?? inventory.refreshing;
  const hasScopedInventory = Boolean(realm || area);
  // A scoped tier summarises exactly what its table rolls up, so the strip and the
  // rows never disagree about the scope the operator is standing in.
  const scopeRollup = hasScopedInventory ? aggregateDomainMetricRows(scopedRows) : null;
  const scopeStats: DomainInventoryStat[] = scopeRollup
    ? [
        ...(area
          ? []
          : [
              {
                label: "Areas",
                value: formatNumber(scopedRealm?.areas.length ?? 0),
              },
            ]),
        { label: "Resources", value: formatNumber(scopedRows.length) },
        ...metricColumns
          .map((column) => ({
            label: column.header,
            value: statValue(column.cell(scopeRollup)),
          }))
          .filter((stat): stat is DomainInventoryStat => stat.value !== null),
      ]
    : [];
  const visibleStats = hasScopedInventory ? scopeStats : stats;
  const freshness = isRefreshing
    ? "Refreshing"
    : !inventory.data && inventory.loading
      ? "Loading"
      : !inventory.data && inventory.error
        ? "Unavailable"
        : inventory.data && inventory.error
          ? "Refresh failed"
          : inventory.data && inventory.stale
            ? "Stale"
            : inventory.data
              ? "Live"
              : undefined;

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          eyebrow={pageEyebrow}
          title={pageTitle}
          description={pageDescription}
          primaryAction={{
            busy: isRefreshing,
            disabled: isRefreshing,
            label: refreshLabel,
            onPress: onRefresh,
          }}
          status={status}
        />
        <OperatorScopeStrip realm={realm} area={area} freshness={freshness} />

        <Show when={!inventory.data && inventory.loading}>
          <QueryLoadingState description={loadingDescription} />
        </Show>

        <Show when={!inventory.data && inventory.error}>
          <QueryErrorState title={errorTitle} error={inventory.error} onRetry={inventory.refresh} />
        </Show>

        <Show when={inventory.data}>
          <Block direction="column" gap="sm">
            <Show when={isRefreshing}>
              <QueryRefreshingState description={refreshingDescription} />
            </Show>
            <Show when={inventory.error}>
              <Alert
                variant="warning"
                title="Refresh failed"
                description={`Showing the last available snapshot. ${formatUnknownError(inventory.error)}`}
                actions={
                  <Button variant="outline" onPress={inventory.refresh}>
                    Retry
                  </Button>
                }
              />
            </Show>
            <Show when={visibleStats.length > 0}>
              <DomainSummaryStrip
                ariaLabel={`${pageTitle} key stats`}
                class="domain-inventory-summary"
                items={visibleStats}
              />
            </Show>
            <Show
              when={area}
              fallback={
                <DomainScopeInventoryTable
                  domain={domain}
                  emptyDescription={emptyDescription}
                  inventory={inventory.data}
                  metricColumns={metricColumns}
                  onSearchChange={(value) => setRouteSearchQuery(ROUTE_SEARCH_QUERY_PARAM, value)}
                  realm={realm}
                  rows={scopedRows}
                  searchValue={searchValue}
                />
              }
            >
              <DomainResourceInventoryTable
                domain={domain}
                emptyDescription={emptyDescription}
                inventory={inventory.data}
                metricColumns={metricColumns}
                title={tableTitle}
              />
            </Show>
          </Block>
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
