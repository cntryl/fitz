import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import type { DomainResourceMetricColumn } from "@/components/shared/domain-resource-inventory-table";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatNumber } from "@/shared/format";

function maybeCount(value: number | undefined) {
  return value === undefined ? "--" : formatNumber(value);
}

export default function RpcPage() {
  const inventory = createResourceInventoryQuery("rpc");
  const rpcMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "pending",
      header: "Pending",
      width: "14%",
      cell: (row) => maybeCount(row.requestsPending),
      sortValue: (row) => row.requestsPending,
    },
    {
      id: "workers",
      priority: "secondary",
      header: "Matching worker registrations",
      width: "16%",
      cell: (row) => maybeCount(row.workersRegistered),
      sortValue: (row) => row.workersRegistered,
      title: () => "Worker registrations can overlap when routes use wildcards.",
    },
    {
      id: "slowest-latency",
      priority: "secondary",
      rollup: "worst",
      header: "Slowest worker avg ms",
      width: "20%",
      cell: (row) =>
        row.slowestWorkerAverageLatencyMs == null
          ? "--"
          : row.slowestWorkerAverageLatencyMs.toFixed(1),
      sortValue: (row) => row.slowestWorkerAverageLatencyMs,
    },
  ];

  return (
    <DomainInventoryPage
      domain="rpc"
      eyebrow="Live request/response"
      title="RPC inventory"
      refreshLabel="Refresh RPC"
      inventory={inventory}
      loadingDescription="Loading RPC inventory..."
      errorTitle="Unable to load RPC inventory"
      refreshingDescription="Refreshing RPC inventory..."
      emptyDescription="No RPC resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={rpcMetricColumns}
      status={inventory.data ? { label: "Route health unavailable", tone: "info" } : undefined}
    />
  );
}
