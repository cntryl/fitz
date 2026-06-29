import { Stack } from "@askrjs/themes/components";
import DomainHeader from "./domain-header";
import type { DomainHeaderProps } from "./domain-header";
import DomainPageFrame from "./domain-page-frame";
import DomainResourceInventoryTable, {
  type DomainResourceInventory,
  type DomainResourceMetricColumn,
} from "./domain-resource-inventory-table";
import { QueryErrorState, QueryLoadingState, QueryRefreshingState } from "./query-state";
import type { DomainSegment } from "@/shared/navigation/domains";

export interface DomainInventoryQuery<TInventory extends DomainResourceInventory> {
  data?: TInventory | null;
  error?: unknown;
  loading?: boolean;
  refresh: () => unknown;
  refreshing?: boolean;
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
  status?: DomainHeaderProps["status"];
  tableTitle: string;
  title: string;
}

function refreshAll(refreshers: Array<() => unknown>) {
  for (const refresh of refreshers) {
    void refresh();
  }
}

export default function DomainInventoryPage<TInventory extends DomainResourceInventory>({
  description,
  domain,
  emptyDescription,
  errorTitle,
  eyebrow,
  inventory,
  loadingDescription,
  metricColumns,
  refreshing,
  refreshers,
  refreshLabel,
  refreshingDescription,
  status,
  tableTitle,
  title,
}: DomainInventoryPageProps<TInventory>) {
  const onRefresh = () => refreshAll(refreshers ?? [inventory.refresh]);
  const isRefreshing = refreshing ?? inventory.refreshing;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow={eyebrow}
          title={title}
          description={description}
          primaryAction={{
            label: refreshLabel,
            onPress: onRefresh,
          }}
          status={status}
        />

        {!inventory.data && inventory.loading ? (
          <QueryLoadingState description={loadingDescription} />
        ) : null}

        {!inventory.data && inventory.error ? (
          <QueryErrorState title={errorTitle} error={inventory.error} onRetry={inventory.refresh} />
        ) : null}

        {inventory.data ? (
          <Stack gap="3">
            {isRefreshing ? <QueryRefreshingState description={refreshingDescription} /> : null}
            <DomainResourceInventoryTable
              domain={domain}
              emptyDescription={emptyDescription}
              inventory={inventory.data}
              metricColumns={metricColumns}
              title={tableTitle}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
