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
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import CommunicationFlowWorkspace from "@/features/communication/communication-flow-workspace";
import {
  createRpcAreaQuery,
  createRpcOverviewQuery,
  createRpcRealmQuery,
} from "@/features/rpc/rpc-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatNumber } from "@/shared/format";
import { domainResourceHref, domainScopeHref } from "@/shared/navigation/domains";

function decodeParam(value: string | undefined) {
  if (!value) return undefined;

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function summarizeRpcHealth(stats: {
  failureTotal: number;
  pendingRoutesActive: number;
  requestTimeoutsTotal: number;
  requestsPending: number;
  workersRegistered: number;
}) {
  const pressureSignals = [
    stats.requestTimeoutsTotal > 0 ? `${stats.requestTimeoutsTotal} request timeout(s)` : null,
    stats.failureTotal > 0 ? `${stats.failureTotal} failure(s)` : null,
    stats.pendingRoutesActive > 0 ? `${stats.pendingRoutesActive} pending route(s)` : null,
    stats.requestsPending > stats.workersRegistered
      ? `${stats.requestsPending - stats.workersRegistered} unassigned request(s)`
      : null,
  ].filter((signal): signal is string => signal !== null);

  const hasCritical = stats.requestTimeoutsTotal > 0 || stats.failureTotal > 0;
  const hasPressure = pressureSignals.length > 0;

  if (hasCritical) {
    return {
      detail: `${stats.requestsPending} pending request(s), ${stats.workersRegistered} registered worker(s). ${pressureSignals.join(", ")}. ${
        stats.requestsPending > 0
          ? "Response reliability deserves immediate attention."
          : "No pending requests are visible."
      }`,
      label: "Attention" as const,
      tone: "danger" as const,
    };
  }

  if (hasPressure) {
    return {
      detail: `${stats.requestsPending} pending request(s), ${stats.workersRegistered} registered worker(s). ${pressureSignals.join(", ")}.`,
      label: "Pressure" as const,
      tone: "warning" as const,
    };
  }

  return {
    detail: `${stats.requestsPending} pending request(s), ${stats.workersRegistered} registered worker(s). Demand is healthy and live.`,
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

function RpcOverviewPage() {
  const overview = createRpcOverviewQuery();
  const inventory = createResourceInventoryQuery("rpc");
  const data = overview.data;
  const health = summarizeRpcHealth(
    data?.stats
      ? {
          failureTotal: data.stats.failureTotal,
          pendingRoutesActive: data.stats.pendingRoutesActive,
          requestTimeoutsTotal: data.stats.requestTimeoutsTotal,
          requestsPending: data.stats.requestsPending,
          workersRegistered: data.stats.workersRegistered,
        }
      : {
          failureTotal: 0,
          pendingRoutesActive: 0,
          requestTimeoutsTotal: 0,
          requestsPending: 0,
          workersRegistered: 0,
        },
  );
  const snapshot = createDomainSidebar({
    data,
    title: "RPC demand snapshot",
    description: "Worker availability and pending request pressure.",
    stats: (current) => [
      { label: "Visible RPC realms", value: current.realms.length },
      {
        label: "Active workers",
        value: current.stats.workersRegistered,
        note: "Live registrations",
      },
      { label: "Requests pending", value: current.stats.requestsPending },
      {
        label: "Pending routes",
        value: current.stats.pendingRoutesActive,
      },
      {
        label: "Request pressure",
        value: current.stats.requestTimeoutsTotal + current.stats.failureTotal,
      },
      {
        label: "Ops / sec",
        value: current.stats.operationsPerSecond.toFixed(2),
        note: "Latest sample",
      },
    ],
  });

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Live request/response"
          title="RPC overview"
          description="Live request/response throughput, pending requests, and worker availability."
          primaryAction={{
            label: "Refresh RPC",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: `${health.detail} Pending work is in-memory and disappears on worker disconnect or broker restart.`,
            label: overview.refreshing ? "Refreshing" : overview.stale ? "Stale" : health.label,
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : health.tone,
          }}
        />

        {snapshot}

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading RPC overview snapshot..." />
        ) : null}

        {!data && overview.error ? (
          <QueryErrorState
            title="RPC overview loading failure"
            error={overview.error}
            onRetry={() => overview.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing RPC overview..." />
            ) : null}

            <CommunicationFlowWorkspace
              domain="rpc"
              error={inventory.error}
              inventory={inventory.data}
              loading={inventory.loading}
              stats={data.stats}
            />

            <DomainMetricTable
              title="RPC metrics"
              description="Live request/response health, worker capacity, and request risk signals."
              metrics={[
                { label: "Requests pending", value: data.stats.requestsPending },
                { label: "Workers registered", value: data.stats.workersRegistered },
                {
                  label: "Pending routes active",
                  value: data.stats.pendingRoutesActive,
                },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
                metricWithRisk(data.stats.requestTimeoutsTotal, "Request timeouts"),
                metricWithRisk(data.stats.failureTotal, "Failure responses"),
                {
                  label: "Closed caller drops",
                  value: data.stats.responsesDroppedClosedCallerTotal,
                },
                { label: "Missing pending", value: data.stats.responsesMissingPendingTotal },
                { label: "Invalid seq responses", value: data.stats.invalidSequenceResponsesTotal },
                {
                  label: "Invalid seq fwd",
                  value: data.stats.invalidSequenceErrorsForwardedTotal,
                },
                {
                  label: "Invalid seq drops",
                  value: data.stats.invalidSequenceErrorsDroppedTotal,
                },
              ]}
            />

            <DomainRealmTable
              domain="rpc"
              title="RPC realms"
              realms={data.realms}
              emptyMessage="No RPC realms are currently visible."
            />

            <DomainWorkflowPanel
              archetype="RPC Communication Flow"
              workflows={[
                "View flow",
                "Inspect participants",
                "Trace failures",
                "Review performance",
              ]}
              questions={[
                "Who talks to whom?",
                "What is failing?",
                "Where is communication breaking down?",
              ]}
              diagnostics={[
                "Pending calls",
                "Worker registrations",
                "Timeout and sequence internals",
              ]}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

function RpcRealmPage(props: { realm: string }) {
  const query = createRpcRealmQuery(props.realm);
  const data = query.data;
  const resourceCount = data?.areas.reduce((sum, area) => sum + area.resources.length, 0) ?? 0;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="RPC realm"
          title={props.realm}
          description={`Area inventory for ${props.realm}.`}
          primaryAction={{ label: "Refresh realm", onPress: () => query.refresh() }}
          status={{
            detail: data
              ? `${data.areas.length} area(s), ${resourceCount} resource(s).`
              : "Loading RPC realm.",
            label: query.refreshing ? "Refreshing" : query.stale ? "Stale" : "Live",
            tone: query.refreshing ? "info" : query.stale ? "warning" : "success",
          }}
        />
        {!data && query.loading ? <QueryLoadingState description="Loading RPC realm..." /> : null}
        {!data && query.error ? (
          <QueryErrorState
            title="Unable to load RPC realm"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        ) : null}
        {data ? (
          <Card padding="sm" variant="default">
            <CardHeader>
              <CardTitle>RPC areas</CardTitle>
              <CardDescription>{data.areas.length} area(s)</CardDescription>
            </CardHeader>
            <CardContent>
              {data.areas.length === 0 ? (
                <QueryEmptyState description="No visible RPC areas at the current level." />
              ) : (
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableHeaderCell>Area</TableHeaderCell>
                      <TableHeaderCell>Resources</TableHeaderCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    <For each={data.areas} by={(area) => `${area.realm}:${area.area}`}>
                      {(area) => (
                        <TableRow>
                          <TableCell>
                            <Link
                              href={domainScopeHref("rpc", { area: area.area, realm: area.realm })}
                            >
                              {area.area}
                            </Link>
                          </TableCell>
                          <TableCell>{formatNumber(area.resources.length)}</TableCell>
                        </TableRow>
                      )}
                    </For>
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

function RpcAreaPage(props: { realm: string; area: string }) {
  const query = createRpcAreaQuery(props.realm, props.area);
  const data = query.data;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="RPC area"
          title={props.area}
          description={`Resources in ${props.realm}/${props.area}.`}
          primaryAction={{ label: "Refresh area", onPress: () => query.refresh() }}
          status={{
            detail: data ? `${data.resources.length} resource(s).` : "Loading RPC area.",
            label: query.refreshing ? "Refreshing" : query.stale ? "Stale" : "Live",
            tone: query.refreshing ? "info" : query.stale ? "warning" : "success",
          }}
        />
        {!data && query.loading ? <QueryLoadingState description="Loading RPC area..." /> : null}
        {!data && query.error ? (
          <QueryErrorState
            title="Unable to load RPC area"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        ) : null}
        {data ? (
          <Card padding="sm" variant="default">
            <CardHeader>
              <CardTitle>RPC resources</CardTitle>
              <CardDescription>{data.resources.length} resource(s)</CardDescription>
            </CardHeader>
            <CardContent>
              {data.resources.length === 0 ? (
                <QueryEmptyState description="No visible RPC resources at the current level." />
              ) : (
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableHeaderCell>Resource</TableHeaderCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    <For each={data.resources} by={(resource) => resource}>
                      {(resource) => (
                        <TableRow>
                          <TableCell>
                            <Link
                              href={domainResourceHref("rpc", {
                                area: props.area,
                                realm: props.realm,
                                resource,
                              })}
                            >
                              {resource}
                            </Link>
                          </TableCell>
                        </TableRow>
                      )}
                    </For>
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

export default function RpcPage() {
  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);

  if (realm && area) return <RpcAreaPage area={area} realm={realm} />;
  if (realm) return <RpcRealmPage realm={realm} />;

  return <RpcOverviewPage />;
}
