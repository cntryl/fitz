import { For, Show } from "@askrjs/askr/control";
import { currentRoute } from "@askrjs/askr/router";
import { Alert, Button, Card, CardContent, Grid, Stack, Text } from "@askrjs/themes/components";
import DomainHeader from "./domain-header";
import type { DomainHeaderProps } from "./domain-header";
import DomainPageFrame from "./domain-page-frame";
import OperatorScopeStrip from "./operator-scope-strip";
import DomainResourceInventoryTable, {
  type DomainResourceInventory,
  type DomainResourceMetricColumn,
} from "./domain-resource-inventory-table";
import { QueryErrorState, QueryLoadingState, QueryRefreshingState } from "./query-state";
import { formatUnknownError } from "@/shared/errors/format";
import { formatDisplayValue } from "@/shared/format";
import type { DomainSegment } from "@/shared/navigation/domains";

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
  stats = [],
  status,
  tableTitle,
  title,
}: DomainInventoryPageProps<TInventory>) {
  const route = currentRoute();
  const onRefresh = () => refreshAll(refreshers ?? [inventory.refresh]);
  const isRefreshing = refreshing ?? inventory.refreshing;
  const hasScopedInventory = Boolean(route.params.realm || route.params.area);
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
      <Stack gap="3">
        <DomainHeader
          eyebrow={eyebrow}
          title={title}
          description={description}
          primaryAction={{
            busy: isRefreshing,
            disabled: isRefreshing,
            label: refreshLabel,
            onPress: onRefresh,
          }}
          status={status}
        />
        <OperatorScopeStrip freshness={freshness} />

        <Show when={!inventory.data && inventory.loading}>
          <QueryLoadingState description={loadingDescription} />
        </Show>

        <Show when={!inventory.data && inventory.error}>
          <QueryErrorState title={errorTitle} error={inventory.error} onRetry={inventory.refresh} />
        </Show>

        <Show when={inventory.data}>
          <Stack gap="3">
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
            <Show when={stats.length > 0 && !hasScopedInventory}>
              <Grid
                class="domain-stat-grid"
                columns={{ base: 1, sm: 2, lg: Math.min(stats.length, 3) }}
                gap="md"
                aria-label={`${title} key stats`}
              >
                <For each={stats as DomainInventoryStat[]} by={(stat) => stat.label}>
                  {(stat) => (
                    <Card key={stat.label} padding="sm" variant="default">
                      <CardContent>
                        <Stack gap="1">
                          <Text as="span" class="domain-header-kicker">
                            {stat.label}
                          </Text>
                          <Text
                            as="strong"
                            class="domain-stat-value"
                            font="mono"
                            numeric="tabular"
                            weight="semibold"
                          >
                            {formatDisplayValue(stat.value)}
                          </Text>
                          {stat.caption ? (
                            <Text as="span" class="domain-muted" size="sm">
                              {stat.caption}
                            </Text>
                          ) : null}
                        </Stack>
                      </CardContent>
                    </Card>
                  )}
                </For>
              </Grid>
            </Show>
            <DomainResourceInventoryTable
              domain={domain}
              emptyDescription={emptyDescription}
              inventory={inventory.data}
              metricColumns={metricColumns}
              title={tableTitle}
            />
          </Stack>
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}
