import { Stack } from "@askrjs/themes/layouts";
import DomainBarChart from "@/components/shared/domain-bar-chart";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createRpcOverviewQuery } from "@/features/rpc/rpc-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

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
          : "No pending requests are queued."
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

export default function RpcPage() {
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
  const sidebar = createDomainSidebar({
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
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Live request/response"
          title="RPC overview"
          description="Live request/response throughput, pending work, and worker availability."
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

            <DomainBarChart
              title="RPC signal"
              description="Worker capacity, pending request pressure, and current throughput."
              label="RPC state snapshot"
              scope="Live RPC snapshot"
              data={[
                {
                  label: "Workers",
                  unitLabel: "workers",
                  value: data.stats.workersRegistered,
                },
                {
                  label: "Pending requests",
                  unitLabel: "requests",
                  value: data.stats.requestsPending,
                },
                {
                  label: "Pending routes",
                  unitLabel: "routes",
                  value: data.stats.pendingRoutesActive,
                },
                {
                  label: "Request timeouts",
                  unitLabel: "timeouts",
                  value: data.stats.requestTimeoutsTotal,
                },
                {
                  label: "Failure pressure",
                  unitLabel: "failures",
                  value: data.stats.failureTotal,
                },
                {
                  label: "Ops / sec",
                  unitLabel: "ops/sec",
                  value: data.stats.operationsPerSecond,
                },
              ]}
            />

            <DomainRealmTable
              title="RPC realms"
              realms={data.realms}
              emptyMessage="No RPC realms are currently visible."
            />

            <DomainResourceBrowser
              domain="rpc"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
