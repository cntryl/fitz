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
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainWorkflowPanel from "@/components/shared/domain-workflow-panel";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import CommunicationFlowWorkspace from "@/features/communication/communication-flow-workspace";
import {
  createNoticeAreaQuery,
  createNoticeOverviewQuery,
  createNoticeRealmQuery,
  createNoticeResourceRowsQuery,
} from "@/features/notice/notice-query";
import type {
  NoticeAreaResourceRows,
  NoticeRealmInventory,
  NoticeResourceOperationRows,
} from "@/features/notice/notice-models";
import { formatNumber } from "@/shared/format";
import { domainResourceHref, domainScopeHref } from "@/shared/navigation/domains";
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
    stats.deliveryDropsTotal > 0 ? `${stats.deliveryDropsTotal} delivery drop(s)` : null,
    stats.wildcardLimitRejectsTotal > 0
      ? `${stats.wildcardLimitRejectsTotal} wildcard reject(s)`
      : null,
  ].filter((signal): signal is string => signal !== null);

  if (hasRisk) {
    return {
      detail: `${stats.subscriptionsActive} active subscriptions and ${stats.publishesPerSecond.toFixed(2)} publishes/sec. ${pressureSignals.join(", ")} are above healthy fanout baseline.`,
      label: "Attention" as const,
      tone: "danger" as const,
    };
  }

  return {
    detail: `${stats.subscriptionsActive} active subscriptions across ${stats.routesActive} route(s). ${stats.publishesPerSecond.toFixed(2)} publishes/sec is moving through live fanout.`,
    label: "Live" as const,
    tone: "success" as const,
  };
}

function metricWithRisk(value: number, label: string) {
  return {
    label,
    value,
    ...(value > 0 ? { caption: "attention" } : undefined),
  };
}

function NoticeAreaTableRows(props: { areas: NoticeRealmInventory["areas"] }) {
  return (
    <For each={props.areas} by={(area) => `${area.realm}/${area.area}`}>
      {(area) => (
        <TableRow>
          <TableCell>
            <Link href={domainScopeHref("notice", { area: area.area, realm: area.realm })}>
              {area.area}
            </Link>
          </TableCell>
          <TableCell>{formatNumber(area.resources.length)}</TableCell>
        </TableRow>
      )}
    </For>
  );
}

function NoticeResourceTableRows(props: { data: NoticeAreaResourceRows }) {
  return (
    <For
      each={props.data.resources}
      by={(resource) => `${props.data.realm}:${props.data.area}:${resource}`}
    >
      {(resource) => (
        <TableRow>
          <TableCell>
            <Link
              href={domainResourceHref("notice", {
                area: props.data.area,
                realm: props.data.realm,
                resource,
              })}
            >
              {resource}
            </Link>
          </TableCell>
        </TableRow>
      )}
    </For>
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

function NoticeOverviewPage() {
  const overview = createNoticeOverviewQuery();
  const inventory = createNoticeOverviewQuery();
  const data = overview.data;
  const health = summarizeNoticeHealth(
    data?.stats ?? {
      deliveryDropsTotal: 0,
      subscriptionsActive: 0,
      wildcardLimitRejectsTotal: 0,
      publishesPerSecond: 0,
      routesActive: 0,
    },
  );
  const snapshot = createDomainSidebar({
    data,
    title: "Notice fanout snapshot",
    description: "Live subscription scope and fanout pressure diagnostics.",
    stats: (current) => [
      { label: "Visible notice realms", value: current.realms.length },
      { label: "Active routes", value: current.stats.routesActive },
      {
        label: "Publish rate",
        value: current.stats.publishesPerSecond.toFixed(2),
        note: "ops/sec",
      },
      { label: "Active subscriptions", value: current.stats.subscriptionsActive },
      {
        label: "Risk indicators",
        value: current.stats.deliveryDropsTotal + current.stats.wildcardLimitRejectsTotal,
      },
      {
        label: "Max route subscribers",
        value: current.stats.maxRouteSubscribers,
      },
    ],
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Live awareness"
          title="Notice overview"
          description="Live fanout health, active subscription scope, and realm coverage."
          primaryAction={{
            label: "Refresh notice",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: `${health.detail} Notice is live fanout only; subscriptions expire on disconnect or restart.`,
            label: overview.refreshing ? "Refreshing" : overview.stale ? "Stale" : health.label,
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : health.tone,
          }}
        />

        {snapshot}

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading notice overview snapshot..." />
        ) : null}

        {!data && overview.error ? (
          <QueryErrorState
            title="Notice overview loading failure"
            error={overview.error}
            onRetry={() => overview.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing notice overview..." />
            ) : null}

            <CommunicationFlowWorkspace
              domain="notice"
              error={inventory.error}
              inventory={null}
              loading={inventory.loading}
              stats={data.stats}
            />

            <DomainMetricTable
              title="Notice metrics"
              description="Live fanout health, publish, and subscription risk signals."
              metrics={[
                { label: "Active subscriptions", value: data.stats.subscriptionsActive },
                { label: "Publish rate", value: data.stats.publishesPerSecond.toFixed(2) },
                metricWithRisk(data.stats.deliveryDropsTotal, "Delivery drops"),
                metricWithRisk(data.stats.wildcardLimitRejectsTotal, "Wildcard limit rejects"),
              ]}
            />

            <DomainRealmTable
              domain="notice"
              title="Notice realms"
              realms={data.realms}
              emptyMessage="No notice realms are currently visible."
            />

            <DomainWorkflowPanel
              archetype="Notice Communication Flow"
              workflows={[
                "View flow",
                "Inspect participants",
                "Inspect drops",
                "Review performance",
              ]}
              questions={[
                "Who talks to whom?",
                "What is failing?",
                "Where is live fanout dropping?",
              ]}
              diagnostics={["Fanout pressure", "Delivery drops", "Subscription internals"]}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

function NoticeRealmPage(props: { realm: string }) {
  const realmQuery = createNoticeRealmQuery(props.realm);
  const data = realmQuery.data;

  const snapshot = createDomainSidebar({
    data,
    title: `Notice realm ${props.realm}`,
    description: props.realm,
    stats: (current) => [
      { label: "Areas", value: current.areas.length },
      {
        label: "Resources",
        value: current.areas.reduce((sum, area) => sum + area.resources.length, 0),
      },
    ],
    footer: <Link href={domainScopeHref("notice")}>Back to overview</Link>,
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Notice realm"
          title={props.realm}
          description={`Area inventory for ${props.realm}.`}
          primaryAction={{ label: "Refresh realm", onPress: () => realmQuery.refresh() }}
          status={{
            detail: data ? `${data.areas.length} visible area(s).` : "Loading notice realm.",
            label: realmQuery.refreshing ? "Refreshing" : realmQuery.stale ? "Stale" : "Live",
            tone: realmQuery.refreshing ? "info" : realmQuery.stale ? "warning" : "success",
          }}
        />

        {snapshot}

        {!data && realmQuery.loading ? (
          <QueryLoadingState description="Loading notice realm..." />
        ) : null}
        {!data && realmQuery.error ? (
          <QueryErrorState
            title="Unable to load notice realm"
            error={realmQuery.error}
            onRetry={() => realmQuery.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {realmQuery.refreshing ? (
              <QueryRefreshingState description="Refreshing notice realm..." />
            ) : null}

            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Notice areas</CardTitle>
                <CardDescription>{data.areas.length} area(s)</CardDescription>
              </CardHeader>
              <CardContent>
                {data.areas.length === 0 ? (
                  <QueryEmptyState description="No visible notice areas at the current level." />
                ) : (
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Area</TableHeaderCell>
                        <TableHeaderCell>Resources</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      <NoticeAreaTableRows areas={data.areas} />
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

function NoticeAreaPage(props: { realm: string; area: string }) {
  const areaQuery = createNoticeAreaQuery(props.realm, props.area);
  const data = areaQuery.data;

  const snapshot = createDomainSidebar({
    data,
    title: `Notice area ${props.area}`,
    description: `${props.realm} / ${props.area}`,
    stats: (current) => [{ label: "Resources", value: current.resources.length }],
    footer: (
      <span>
        <Link href={domainScopeHref("notice", { realm: props.realm })}>Back to realm</Link>
        <Link href={domainScopeHref("notice")}>Back to overview</Link>
      </span>
    ),
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Notice area"
          title={props.area}
          description={`Resources in ${props.realm}/${props.area}.`}
          primaryAction={{ label: "Refresh area", onPress: () => areaQuery.refresh() }}
          status={{
            detail: data ? `${data.resources.length} resource(s).` : "Loading notice area.",
            label: areaQuery.refreshing ? "Refreshing" : areaQuery.stale ? "Stale" : "Live",
            tone: areaQuery.refreshing ? "info" : areaQuery.stale ? "warning" : "success",
          }}
        />

        {snapshot}

        {!data && areaQuery.loading ? (
          <QueryLoadingState description="Loading notice area..." />
        ) : null}
        {!data && areaQuery.error ? (
          <QueryErrorState
            title="Unable to load notice area"
            error={areaQuery.error}
            onRetry={() => areaQuery.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {areaQuery.refreshing ? (
              <QueryRefreshingState description="Refreshing notice area..." />
            ) : null}
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Notice resources</CardTitle>
                <CardDescription>{data.resources.length} resource(s)</CardDescription>
              </CardHeader>
              <CardContent>
                {data.resources.length === 0 ? (
                  <QueryEmptyState description="No visible notice resources at the current level." />
                ) : (
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Resource</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      <NoticeResourceTableRows data={data} />
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
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
        Back to area
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

  if (realm && area) {
    return <NoticeAreaPage area={area} realm={realm} />;
  }

  if (realm) {
    return <NoticeRealmPage realm={realm} />;
  }

  return <NoticeOverviewPage />;
}
