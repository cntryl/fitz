import { Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import { Alert, Button, Block } from "@askrjs/themes/components";
import DomainHeader from "./domain-header";
import type { DomainHeaderProps } from "./domain-header";
import DomainPageFrame from "./domain-page-frame";
import DomainScopeInventoryTable from "./domain-scope-inventory-table";
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
import { domainTitleForSegment, type DomainSegment } from "@/shared/navigation/domains";

export interface DomainInventoryQuery<TInventory extends DomainResourceInventory> {
  data?: TInventory | null;
  error?: unknown;
  loading?: boolean;
  refresh: () => unknown;
  refreshing?: boolean;
  stale?: boolean;
}

export interface DomainInventoryPageProps<TInventory extends DomainResourceInventory> {
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
export default function DomainInventoryPage<TInventory extends DomainResourceInventory>({
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
  const pageTitle = area ?? realm ?? title;
  const pageEyebrow = area ? `${domainTitle} area` : realm ? `${domainTitle} realm` : eyebrow;
  const onRefresh = () => refreshAll(refreshers ?? [inventory.refresh]);
  const isRefreshing = refreshing ?? inventory.refreshing;
  const hasScopedInventory = Boolean(realm || area);
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
  const freshnessBadge =
    freshness && freshness !== "Live"
      ? {
          label: freshness,
          tone:
            freshness === "Refreshing" || freshness === "Loading"
              ? ("info" as const)
              : ("warning" as const),
        }
      : undefined;

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          compact={true}
          eyebrow={pageEyebrow}
          title={pageTitle}
          freshness={freshnessBadge}
          primaryAction={{
            busy: isRefreshing,
            disabled: isRefreshing,
            label: refreshLabel,
            onPress: onRefresh,
          }}
          status={hasScopedInventory ? undefined : status}
        />

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
