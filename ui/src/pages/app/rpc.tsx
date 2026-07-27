import DomainInventoryPage from "@/components/shared/domain-inventory-page";
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
  const unassignedRequests = Math.max(0, stats.requestsPending - stats.workersRegistered);
  const baseDetail = `${formatNumber(stats.requestsPending)} pending ${
    stats.requestsPending === 1 ? "request" : "requests"
  }, ${formatNumber(
    stats.workersRegistered,
  )} registered ${stats.workersRegistered === 1 ? "worker" : "workers"}, ${formatNumber(
    stats.pendingRoutesActive,
  )} ${stats.pendingRoutesActive === 1 ? "route has" : "routes have"} pending work.`;
  const historyDetail = `Cumulative process totals: ${formatNumber(
    stats.requestTimeoutsTotal,
  )} ${stats.requestTimeoutsTotal === 1 ? "timeout" : "timeouts"}, ${formatNumber(
    stats.failureTotal,
  )} ${stats.failureTotal === 1 ? "failure" : "failures"}.`;

  if (unassignedRequests > 0) {
    return {
      detail: `${baseDetail} ${formatNumber(unassignedRequests)} ${
        unassignedRequests === 1 ? "request is" : "requests are"
      } not covered by a registered worker. ${historyDetail} Historical totals do not identify a current incident.`,
      label: "Pressure" as const,
      tone: "warning" as const,
    };
  }

  if (stats.requestsPending > 0 || stats.pendingRoutesActive > 0) {
    return {
      detail: `${baseDetail} Registered workers cover the current pending count. ${historyDetail} Historical totals do not identify a current incident.`,
      label: "Active" as const,
      tone: "info" as const,
    };
  }

  return {
    detail: `${baseDetail} No pending demand is visible. ${historyDetail} Historical totals do not identify a current incident.`,
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
  return (
    <DomainInventoryPage
      domain="rpc"
      eyebrow="Live request/response"
      title="RPC inventory"
      description="Live request/response resources for the active Route Family."
      refreshLabel="Refresh RPC"
      inventory={inventory}
      refreshing={overview.refreshing || inventory.refreshing}
      refreshers={[() => overview.refresh(), () => inventory.refresh()]}
      loadingDescription="Loading RPC inventory..."
      errorTitle="Unable to load RPC inventory"
      refreshingDescription="Refreshing RPC inventory..."
      emptyDescription="No RPC resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
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
