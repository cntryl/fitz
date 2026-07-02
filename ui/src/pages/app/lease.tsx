import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import type { DomainResourceMetricColumn } from "@/components/shared/domain-resource-inventory-table";
import { createLeaseOverviewQuery } from "@/features/lease/lease-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatDurationSeconds, formatNumber } from "@/shared/format";

function riskSignal(stats: {
  acquireTimeoutsTotal: number;
  forcedReleasesTotal: number;
  invalidTokenRejectsTotal: number;
  oldestLeaseAgeSeconds: number;
  leasesActive: number;
  waiterDepth: number;
}) {
  const pressureSignals =
    stats.acquireTimeoutsTotal + stats.forcedReleasesTotal + stats.invalidTokenRejectsTotal;
  const pressureCount = pressureSignals + stats.waiterDepth;
  const riskBits = [
    stats.acquireTimeoutsTotal > 0 ? `${stats.acquireTimeoutsTotal} acquire timeout(s)` : null,
    stats.forcedReleasesTotal > 0 ? `${stats.forcedReleasesTotal} forced release(s)` : null,
    stats.invalidTokenRejectsTotal > 0 ? `${stats.invalidTokenRejectsTotal} token reject(s)` : null,
  ].filter(Boolean) as string[];
  const pressureDetail = `${formatNumber(
    stats.acquireTimeoutsTotal,
  )} acquire timeout(s), ${formatNumber(stats.forcedReleasesTotal)} forced release(s), ${formatNumber(
    stats.invalidTokenRejectsTotal,
  )} token reject(s)`;
  const detailBase = `${formatNumber(stats.leasesActive)} active leases, ${formatNumber(
    stats.waiterDepth,
  )} waiters, ${formatDurationSeconds(
    stats.oldestLeaseAgeSeconds,
  )} oldest lease age. Pressure counters: ${pressureDetail}.`;

  if (pressureCount > 6) {
    return {
      detail: `${detailBase} Attention is warranted from ${riskBits.join(", ")}.`,
      label: "Attention" as const,
      tone: "danger" as const,
    };
  }

  if (pressureSignals > 0 || stats.waiterDepth > 0) {
    return {
      detail: `${detailBase} ${stats.waiterDepth ? `Waiters visible (${formatNumber(stats.waiterDepth)}). ` : ""}${
        riskBits.length > 0 ? `Current risk signals: ${riskBits.join(", ")}.` : ""
      }`,
      label: "Pressure" as const,
      tone: "warning" as const,
    };
  }

  return {
    detail: `${detailBase} No immediate lease contention risk is visible.`,
    label: "Live" as const,
    tone: "success" as const,
  };
}

export default function LeasePage() {
  const overview = createLeaseOverviewQuery();
  const inventory = createResourceInventoryQuery("lease");
  const stats = overview.data?.stats;
  const health =
    overview.data &&
    riskSignal({
      acquireTimeoutsTotal: overview.data.stats.acquireTimeoutsTotal,
      forcedReleasesTotal: overview.data.stats.forcedReleasesTotal,
      invalidTokenRejectsTotal: overview.data.stats.invalidTokenRejectsTotal,
      oldestLeaseAgeSeconds: overview.data.stats.oldestLeaseAgeSeconds,
      leasesActive: overview.data.stats.leasesActive,
      waiterDepth: overview.data.stats.waiterDepth,
    });
  const leaseMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "active-leases",
      header: "Active leases",
      width: "12%",
      cell: () => (stats ? formatNumber(stats.leasesActive) : "--"),
    },
    {
      id: "waiters",
      header: "Waiters",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.waiterDepth) : "--"),
    },
    {
      id: "oldest-age",
      header: "Oldest age",
      width: "11%",
      cell: () => (stats ? formatDurationSeconds(stats.oldestLeaseAgeSeconds) : "--"),
    },
    {
      id: "ops",
      header: "Ops / sec",
      width: "10%",
      cell: () => (stats ? stats.operationsPerSecond.toFixed(2) : "--"),
    },
    {
      id: "pressure",
      header: "Pressure",
      width: "10%",
      cell: () =>
        stats
          ? formatNumber(
              stats.acquireTimeoutsTotal +
                stats.forcedReleasesTotal +
                stats.invalidTokenRejectsTotal,
            )
          : "--",
    },
  ];

  return (
    <DomainInventoryPage
      domain="lease"
      eyebrow="Ownership coordination"
      title="Lease inventory"
      description="Ephemeral ownership coordination resources for the active route family."
      refreshLabel="Refresh lease"
      inventory={inventory}
      refreshing={overview.refreshing || inventory.refreshing}
      refreshers={[() => overview.refresh(), () => inventory.refresh()]}
      loadingDescription="Loading lease inventory..."
      errorTitle="Unable to load lease inventory"
      refreshingDescription="Refreshing lease inventory..."
      emptyDescription="No lease resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={leaseMetricColumns}
      stats={[
        { label: "Active leases", value: stats ? formatNumber(stats.leasesActive) : "--" },
        { label: "Waiters", value: stats ? formatNumber(stats.waiterDepth) : "--" },
        {
          label: "Oldest lease",
          value: stats ? formatDurationSeconds(stats.oldestLeaseAgeSeconds) : "--",
        },
      ]}
      status={{
        detail: overview.data
          ? (health?.detail ?? "")
          : overview.error
            ? "Lease health is unavailable. Resource inventory can still be inspected when loaded."
            : "Loading lease health.",
        label: overview.refreshing
          ? "Refreshing"
          : overview.error
            ? "Health unavailable"
            : overview.stale
              ? "Stale"
              : (health?.label ?? "Loading"),
        tone: overview.refreshing
          ? "info"
          : overview.error
            ? "warning"
            : overview.stale
              ? "warning"
              : (health?.tone ?? "info"),
      }}
    />
  );
}
