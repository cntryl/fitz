import { For, Show } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Stack,
} from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import type { DomainResourceMetricColumn } from "@/components/shared/domain-resource-inventory-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { queryFreshness, queryHeaderStatus } from "@/components/shared/query-header-status";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import {
  createNoticeOverviewQuery,
  createNoticeResourceRowsQuery,
} from "@/features/notice/notice-query";
import type { NoticeResourceOperationRows } from "@/features/notice/notice-models";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatCount, formatNumber } from "@/shared/format";
import { domainScopeHref, formatFitzRoute } from "@/shared/navigation/domains";
import NoticeOperationPage from "./notice-operation";

const noticeMetricColumns: readonly DomainResourceMetricColumn[] = [
  {
    id: "subscriptions",
    header: "Subscriptions",
    width: "16%",
    cell: (row) => formatNumber(row.subscriptionsActive ?? 0),
    sortValue: (row) => row.subscriptionsActive,
  },
  {
    id: "publishes",
    header: "Publishes / min",
    width: "16%",
    cell: (row) => (row.publishesPerMinute ?? 0).toFixed(2),
    sortValue: (row) => row.publishesPerMinute,
  },
  {
    id: "delivered",
    header: "Delivered",
    width: "14%",
    cell: (row) => formatNumber(row.notificationsReceived ?? 0),
    sortValue: (row) => row.notificationsReceived,
  },
];

function decodeParam(value: string | undefined) {
  if (!value) return undefined;

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function parseLimit(value: string | null) {
  if (!value) return 50;

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 50;

  return Math.max(1, Math.min(200, Math.floor(parsed)));
}

function summarizeNoticeHealth(stats: {
  deliveryDropsTotal: number;
  subscriptionsActive: number;
  wildcardLimitRejectsTotal: number;
  publishesPerSecond: number;
  routesActive: number;
}) {
  const baseDetail = `${formatNumber(stats.routesActive)} active operation ${
    stats.routesActive === 1 ? "route" : "routes"
  }, ${formatNumber(
    stats.subscriptionsActive,
  )} active ${stats.subscriptionsActive === 1 ? "subscription" : "subscriptions"}, ${stats.publishesPerSecond.toFixed(2)} publishes/sec.`;
  const historicalDetail = `Cumulative process totals: ${formatNumber(
    stats.deliveryDropsTotal,
  )} delivery ${stats.deliveryDropsTotal === 1 ? "drop" : "drops"}, ${formatNumber(
    stats.wildcardLimitRejectsTotal,
  )} wildcard ${stats.wildcardLimitRejectsTotal === 1 ? "rejection" : "rejections"}.`;

  return {
    detail: `${baseDetail} ${historicalDetail} Historical totals do not identify a current fanout incident.`,
    label: "Live" as const,
    tone:
      stats.subscriptionsActive > 0 || stats.publishesPerSecond > 0
        ? ("success" as const)
        : ("info" as const),
  };
}

function NoticeLandingPage() {
  const overview = createNoticeOverviewQuery();
  const inventory = createResourceInventoryQuery("notice");
  const health = summarizeNoticeHealth(
    overview.data?.stats ?? {
      deliveryDropsTotal: 0,
      subscriptionsActive: 0,
      wildcardLimitRejectsTotal: 0,
      publishesPerSecond: 0,
      routesActive: 0,
    },
  );
  const stats = overview.data?.stats;
  return (
    <DomainInventoryPage
      domain="notice"
      eyebrow="Live awareness"
      title="Notice inventory"
      description="Live fanout resources for the active Route Family."
      refreshLabel="Refresh notice"
      inventory={inventory}
      refreshing={overview.refreshing || inventory.refreshing}
      refreshers={[() => overview.refresh(), () => inventory.refresh()]}
      loadingDescription="Loading notice inventory..."
      errorTitle="Unable to load notice inventory"
      refreshingDescription="Refreshing notice inventory..."
      emptyDescription="No notice resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={noticeMetricColumns}
      stats={[
        {
          label: "Active operation routes",
          value: stats ? formatNumber(stats.routesActive) : "--",
        },
        { label: "Subscriptions", value: stats ? formatNumber(stats.subscriptionsActive) : "--" },
        {
          label: "Publishes / sec",
          value: stats ? stats.publishesPerSecond.toFixed(2) : "--",
        },
      ]}
      status={queryHeaderStatus(
        overview,
        {
          loading: "Loading notice health.",
          ready: `${health.detail} Notice is live fanout; subscriptions expire on disconnect or restart.`,
          unavailable:
            "Notice health is unavailable. Resource inventory can still be inspected when loaded.",
        },
        { label: health.label, tone: health.tone },
      )}
    />
  );
}

function NoticeOperationTableRows(props: { data: NoticeResourceOperationRows }) {
  return (
    <For each={props.data.operations} by={(row) => row.operation}>
      {(row) => {
        const route = row.operation.startsWith("notice://")
          ? row.operation
          : formatFitzRoute("notice", {
              area: props.data.area,
              operation: row.operation,
              realm: props.data.realm,
              resource: props.data.resource,
            });

        return (
          <TableRow>
            <TableCell>
              <Link
                class="domain-link-cell"
                href={domainScopeHref("notice", {
                  area: props.data.area,
                  realm: props.data.realm,
                  resource: props.data.resource,
                  operation: row.operation,
                })}
                title={route}
              >
                {route}
              </Link>
            </TableCell>
            <TableCell>{formatNumber(row.activeSubscribers)}</TableCell>
            <TableCell>{formatNumber(row.rollingMessageCount)}</TableCell>
          </TableRow>
        );
      }}
    </For>
  );
}

function NoticeResourcePage(props: { realm: string; area: string; resource: string }) {
  const route = currentRoute();
  const limit = parseLimit(route.query.get("limit"));
  const rowsQuery = createNoticeResourceRowsQuery({
    area: props.area,
    limit,
    realm: props.realm,
    resource: props.resource,
  });
  const data = rowsQuery.data;

  const totalSubscribers = (data?.operations ?? []).reduce(
    (sum, row) => sum + row.activeSubscribers,
    0,
  );
  const totalMessages = (data?.operations ?? []).reduce(
    (sum, row) => sum + row.rollingMessageCount,
    0,
  );

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Notice resource"
          title="Notice operations"
          description={`${props.realm} / ${props.area} / ${props.resource}`}
          primaryAction={{
            busy: rowsQuery.refreshing,
            disabled: rowsQuery.refreshing,
            label: "Refresh operations",
            onPress: () => rowsQuery.refresh(),
          }}
          status={queryHeaderStatus(rowsQuery, {
            loading: "Loading notice operations for this resource.",
            ready: data
              ? `${formatCount(data.operations.length, "operation route")}, ${formatCount(
                  totalSubscribers,
                  "active subscriber",
                )}, ${formatCount(totalMessages, "publish", "publishes")}/min.`
              : "",
            unavailable: "Notice operations are unavailable for this resource.",
          })}
        />
        <OperatorScopeStrip
          realm={props.realm}
          area={props.area}
          resource={props.resource}
          freshness={queryFreshness(rowsQuery)}
        />
        <Show when={!data && rowsQuery.loading}>
          <QueryLoadingState description="Loading notice operation rows..." />
        </Show>
        <Show when={!data && rowsQuery.error}>
          <QueryErrorState
            title="Unable to load notice operation rows"
            error={rowsQuery.error}
            onRetry={() => rowsQuery.refresh()}
          />
        </Show>

        <Show when={data}>
          <Stack gap="3">
            <Show when={rowsQuery.refreshing}>
              <QueryRefreshingState description="Refreshing operation summary..." />
            </Show>

            <Show
              when={data && data.operations.length === 0}
              fallback={
                data ? (
                  <Card padding="sm" variant="default">
                    <CardHeader>
                      <CardTitle titleAs="h2">Notice operations</CardTitle>
                      <CardDescription>
                        Live operation routes with active subscribers and route publish rates.
                      </CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div class="domain-table-wrap notice-resource-table-wrap">
                        <Table>
                          <TableHead>
                            <TableRow>
                              <TableHeaderCell>Route</TableHeaderCell>
                              <TableHeaderCell>Active subscribers</TableHeaderCell>
                              <TableHeaderCell>Publishes / min</TableHeaderCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            <NoticeOperationTableRows data={data} />
                          </TableBody>
                        </Table>
                      </div>
                    </CardContent>
                  </Card>
                ) : null
              }
            >
              <QueryEmptyState description="No matching notice operations are currently visible." />
            </Show>
          </Stack>
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}

export default function NoticePage() {
  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);
  const resource = decodeParam(route.params.resource);
  const operation = decodeParam(route.params.operation);

  if (realm && area && resource && operation) {
    return (
      <NoticeOperationPage area={area} operation={operation} realm={realm} resource={resource} />
    );
  }

  if (realm && area && resource) {
    return <NoticeResourcePage area={area} realm={realm} resource={resource} />;
  }

  return <NoticeLandingPage />;
}
