import { Card, CardContent, Grid, Stack, Text } from "@askrjs/themes/components";
import DomainHeader from "./domain-header";
import type { DomainHeaderProps } from "./domain-header";
import DomainPageFrame from "./domain-page-frame";
import OperatorScopeStrip from "./operator-scope-strip";
import DomainResourceInventoryTable, {
  type DomainResourceInventory,
  type DomainResourceMetricColumn,
} from "./domain-resource-inventory-table";
import { QueryErrorState, QueryLoadingState, QueryRefreshingState } from "./query-state";
import { formatDisplayValue } from "@/shared/format";
import type { DomainSegment } from "@/shared/navigation/domains";

export interface DomainInventoryQuery<TInventory extends DomainResourceInventory> {
  data?: TInventory | null;
  error?: unknown;
  loading?: boolean;
  refresh: () => unknown;
  refreshing?: boolean;
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
  const onRefresh = () => refreshAll(refreshers ?? [inventory.refresh]);
  const isRefreshing = refreshing ?? inventory.refreshing;
  const freshness = isRefreshing
    ? "Refreshing"
    : !inventory.data && inventory.loading
      ? "Loading"
      : !inventory.data && inventory.error
        ? "Unavailable"
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
            label: refreshLabel,
            onPress: onRefresh,
          }}
          status={status}
        />
        <OperatorScopeStrip freshness={freshness} />

        {!inventory.data && inventory.loading ? (
          <QueryLoadingState description={loadingDescription} />
        ) : null}

        {!inventory.data && inventory.error ? (
          <QueryErrorState title={errorTitle} error={inventory.error} onRetry={inventory.refresh} />
        ) : null}

        {inventory.data ? (
          <Stack gap="3">
            {isRefreshing ? <QueryRefreshingState description={refreshingDescription} /> : null}
            {stats.length > 0 ? (
              <Grid
                class="domain-stat-grid"
                columns={{ base: 1, sm: 2, lg: Math.min(stats.length, 3) }}
                gap="md"
                aria-label={`${title} key stats`}
              >
                {stats.map((stat) => (
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
                ))}
              </Grid>
            ) : null}
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
