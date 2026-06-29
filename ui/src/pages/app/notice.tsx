import { For } from "@askrjs/askr/control";
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
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
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
import { formatNumber } from "@/shared/format";
import { domainScopeHref } from "@/shared/navigation/domains";
import NoticeOperationPage from "./notice-operation";

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

function formatLatency(latencyMs: number | null) {
  if (latencyMs == null) return "--/N/A";
  return `${formatNumber(latencyMs)} ms`;
}

function summarizeNoticeHealth(stats: {
  deliveryDropsTotal: number;
  subscriptionsActive: number;
  wildcardLimitRejectsTotal: number;
  publishesPerSecond: number;
  routesActive: number;
}) {
  const riskCount = stats.deliveryDropsTotal + stats.wildcardLimitRejectsTotal;
  const hasRisk = riskCount > 0;
  const pressureSignals = [
    stats.deliveryDropsTotal > 0
      ? `${formatNumber(stats.deliveryDropsTotal)} delivery drop(s)`
      : null,
    stats.wildcardLimitRejectsTotal > 0
      ? `${formatNumber(stats.wildcardLimitRejectsTotal)} wildcard reject(s)`
      : null,
  ].filter((signal): signal is string => signal !== null);

  if (hasRisk) {
    return {
      detail: `${formatNumber(stats.subscriptionsActive)} active subscriptions and ${stats.publishesPerSecond.toFixed(2)} publishes/sec. ${pressureSignals.join(", ")} are above healthy fanout baseline.`,
      label: "Attention" as const,
      tone: "danger" as const,
    };
  }

  return {
    detail: `${formatNumber(stats.subscriptionsActive)} active subscriptions across ${formatNumber(
      stats.routesActive,
    )} route(s). ${stats.publishesPerSecond.toFixed(2)} publishes/sec is moving through live fanout.`,
    label: "Live" as const,
    tone: "success" as const,
  };
}

function resourceCount(data: ReturnType<typeof createResourceInventoryQuery>["data"]) {
  return (
    data?.realms.reduce(
      (sum, realm) =>
        sum + realm.areas.reduce((areaSum, area) => areaSum + area.resources.length, 0),
      0,
    ) ?? 0
  );
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
  const noticeCount = resourceCount(inventory.data);
  const stats = overview.data?.stats;
  const noticeMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "subscriptions",
      header: "Subscriptions",
      width: "12%",
      cell: () => (stats ? formatNumber(stats.subscriptionsActive) : "--"),
    },
    {
      id: "routes",
      header: "Routes",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.routesActive) : "--"),
    },
    {
      id: "publishes",
      header: "Publishes / sec",
      width: "12%",
      cell: () => (stats ? stats.publishesPerSecond.toFixed(2) : "--"),
    },
    {
      id: "drops",
      header: "Drops",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.deliveryDropsTotal) : "--"),
    },
    {
      id: "wildcard-rejects",
      header: "Wildcard rejects",
      width: "13%",
      cell: () => (stats ? formatNumber(stats.wildcardLimitRejectsTotal) : "--"),
    },
  ];

  return (
    <DomainInventoryPage
      domain="notice"
      eyebrow="Live awareness"
      title="Notice inventory"
      description="Live fanout resources for the active route family."
      refreshLabel="Refresh notice"
      inventory={inventory}
      refreshing={overview.refreshing || inventory.refreshing}
      refreshers={[() => overview.refresh(), () => inventory.refresh()]}
      loadingDescription="Loading notice inventory..."
      errorTitle="Unable to load notice inventory"
      refreshingDescription="Refreshing notice inventory..."
      emptyDescription="No notice resources are currently visible."
      tableTitle="Resource inventory"
      metricColumns={noticeMetricColumns}
      status={{
        detail: overview.data
          ? `${formatNumber(noticeCount)} notice resource${noticeCount === 1 ? "" : "s"} visible. ${
              health.detail
            } Notice is live fanout only; subscriptions expire on disconnect or restart.`
          : overview.error
            ? "Notice health is unavailable. Resource inventory can still be inspected when loaded."
            : "Loading notice health.",
        label: overview.refreshing
          ? "Refreshing"
          : overview.error
            ? "Health unavailable"
            : overview.stale
              ? "Stale"
              : health.label,
        tone: overview.refreshing
          ? "info"
          : overview.error
            ? "warning"
            : overview.stale
              ? "warning"
              : health.tone,
      }}
    />
  );
}

function NoticeOperationTableRows(props: { data: NoticeResourceOperationRows }) {
  return (
    <For each={props.data.operations} by={(row) => row.operation}>
      {(row) => (
        <TableRow>
          <TableCell>
            <Link
              href={domainScopeHref("notice", {
                area: props.data.area,
                realm: props.data.realm,
                resource: props.data.resource,
                operation: row.operation,
              })}
            >
              {row.operation}
            </Link>
          </TableCell>
          <TableCell>{formatNumber(row.activeSubscribers)}</TableCell>
          <TableCell>{formatNumber(row.rollingMessageCount)}</TableCell>
          <TableCell>{formatLatency(row.latencyMs)}</TableCell>
        </TableRow>
      )}
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

  const snapshot = createDomainSidebar({
    data,
    title: `Notice operations for ${props.resource}`,
    description: `${props.realm} / ${props.area} / ${props.resource}`,
    stats: (current) => [
      { label: "Operations", value: current.operations.length },
      { label: "Active subscriptions", value: totalSubscribers },
      { label: "Rolling messages / min", value: totalMessages },
    ],
    footer: (
      <Link href={domainScopeHref("notice", { area: props.area, realm: props.realm })}>
        Back to inventory
      </Link>
    ),
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Notice resource"
          title="Notice operations"
          description={`${props.realm} / ${props.area} / ${props.resource}`}
          primaryAction={{ label: "Refresh operations", onPress: () => rowsQuery.refresh() }}
          status={{
            detail: data
              ? `${data.operations.length} operation route(s) currently visible.`
              : "Loading notice operations for this resource.",
            label: rowsQuery.refreshing ? "Refreshing" : rowsQuery.stale ? "Stale" : "Live",
            tone: rowsQuery.refreshing ? "info" : rowsQuery.stale ? "warning" : "success",
          }}
        />

        {snapshot}

        {!data && rowsQuery.loading ? (
          <QueryLoadingState description="Loading notice operation rows..." />
        ) : null}
        {!data && rowsQuery.error ? (
          <QueryErrorState
            title="Unable to load notice operation rows"
            error={rowsQuery.error}
            onRetry={() => rowsQuery.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {rowsQuery.refreshing ? (
              <QueryRefreshingState description="Refreshing operation summary..." />
            ) : null}

            {data.operations.length === 0 ? (
              <QueryEmptyState description="No matching notice operations are currently visible." />
            ) : (
              <Card padding="sm" variant="default">
                <CardHeader>
                  <CardTitle>Notice operations</CardTitle>
                  <CardDescription>Rows are grouped by operation route.</CardDescription>
                </CardHeader>
                <CardContent>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Operation</TableHeaderCell>
                        <TableHeaderCell>Active subscribers</TableHeaderCell>
                        <TableHeaderCell>Rolling messages / min</TableHeaderCell>
                        <TableHeaderCell>Latency</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      <NoticeOperationTableRows data={data} />
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
            )}
          </Stack>
        ) : null}
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
