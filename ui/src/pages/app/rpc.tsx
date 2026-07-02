import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import type { DomainResourceMetricColumn } from "@/components/shared/domain-resource-inventory-table";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { createRpcOverviewQuery } from "@/features/rpc/rpc-query";
import { formatNumber } from "@/shared/format";

function summarizeRpcHealth(stats: {
  failureTotal: number;
  pendingRoutesActive: number;
  requestTimeoutsTotal: number;
  requestsPending: number;
  workersRegistered: number;
}) {
  const pressureSignals = [
    stats.requestTimeoutsTotal > 0
      ? `${formatNumber(stats.requestTimeoutsTotal)} request timeout(s)`
      : null,
    stats.failureTotal > 0 ? `${formatNumber(stats.failureTotal)} failure(s)` : null,
    stats.pendingRoutesActive > 0
      ? `${formatNumber(stats.pendingRoutesActive)} pending route(s)`
      : null,
    stats.requestsPending > stats.workersRegistered
      ? `${formatNumber(stats.requestsPending - stats.workersRegistered)} unassigned request(s)`
      : null,
  ].filter((signal): signal is string => signal !== null);

  const hasCritical = stats.requestTimeoutsTotal > 0 || stats.failureTotal > 0;
  const hasPressure = pressureSignals.length > 0;
  const baseDetail = `${formatNumber(stats.requestsPending)} pending request(s), ${formatNumber(
    stats.workersRegistered,
  )} registered worker(s), ${formatNumber(
    stats.pendingRoutesActive,
  )} pending route(s), ${formatNumber(
    stats.requestTimeoutsTotal,
  )} timeout(s), ${formatNumber(stats.failureTotal)} failure(s).`;

  if (hasCritical) {
    return {
      detail: `${baseDetail} ${pressureSignals.join(", ")}. ${
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
      detail: `${baseDetail} ${pressureSignals.join(", ")}.`,
      label: "Pressure" as const,
      tone: "warning" as const,
    };
  }

  return {
    detail: `${baseDetail} Demand is healthy and live.`,
    label: "Live" as const,
    tone: "success" as const,
  };
}

export default function RpcPage() {
  const overview = createRpcOverviewQuery();
  const inventory = createResourceInventoryQuery("rpc");
  const health = summarizeRpcHealth(
    overview.data?.stats
      ? {
          failureTotal: overview.data.stats.failureTotal,
          pendingRoutesActive: overview.data.stats.pendingRoutesActive,
          requestTimeoutsTotal: overview.data.stats.requestTimeoutsTotal,
          requestsPending: overview.data.stats.requestsPending,
          workersRegistered: overview.data.stats.workersRegistered,
        }
      : {
          failureTotal: 0,
          pendingRoutesActive: 0,
          requestTimeoutsTotal: 0,
          requestsPending: 0,
          workersRegistered: 0,
        },
  );
  const stats = overview.data?.stats;
  const rpcMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "pending",
      header: "Pending",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.requestsPending) : "--"),
    },
    {
      id: "workers",
      header: "Workers",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.workersRegistered) : "--"),
    },
    {
      id: "pending-routes",
      header: "Pending routes",
      width: "13%",
      cell: () => (stats ? formatNumber(stats.pendingRoutesActive) : "--"),
    },
    {
      id: "timeouts",
      header: "Timeouts",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.requestTimeoutsTotal) : "--"),
    },
    {
      id: "failures",
      header: "Failures",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.failureTotal) : "--"),
    },
  ];

  return (
    <DomainInventoryPage
      domain="rpc"
      eyebrow="Live request/response"
      title="RPC inventory"
      description="Live request/response resources for the active route family."
      refreshLabel="Refresh RPC"
      inventory={inventory}
      refreshing={overview.refreshing || inventory.refreshing}
      refreshers={[() => overview.refresh(), () => inventory.refresh()]}
      loadingDescription="Loading RPC inventory..."
      errorTitle="Unable to load RPC inventory"
      refreshingDescription="Refreshing RPC inventory..."
      emptyDescription="No RPC resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={rpcMetricColumns}
      stats={[
        { label: "Pending", value: stats ? formatNumber(stats.requestsPending) : "--" },
        { label: "Workers", value: stats ? formatNumber(stats.workersRegistered) : "--" },
        { label: "Ops / sec", value: stats ? stats.operationsPerSecond.toFixed(2) : "--" },
      ]}
      status={{
        detail: overview.data
          ? `${health.detail} Pending work is in-memory and disappears on worker disconnect or broker restart.`
          : overview.error
            ? "RPC health is unavailable. Resource inventory can still be inspected when loaded."
            : "Loading RPC health.",
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
