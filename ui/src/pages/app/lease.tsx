import DomainInventoryPage from "@/components/shared/domain-inventory-page";
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
  const cumulativeDetail = `${formatNumber(
    stats.acquireTimeoutsTotal,
  )} acquire ${stats.acquireTimeoutsTotal === 1 ? "timeout" : "timeouts"}, ${formatNumber(
    stats.forcedReleasesTotal,
  )} forced ${stats.forcedReleasesTotal === 1 ? "release" : "releases"}, ${formatNumber(
    stats.invalidTokenRejectsTotal,
  )} token ${stats.invalidTokenRejectsTotal === 1 ? "rejection" : "rejections"}`;
  const detailBase = `${formatNumber(stats.leasesActive)} active leases, ${formatNumber(
    stats.waiterDepth,
  )} waiters, ${formatDurationSeconds(
    stats.oldestLeaseAgeSeconds,
  )} oldest lease age. Cumulative process totals: ${cumulativeDetail}.`;

  if (stats.waiterDepth > 0) {
    return {
      detail: `${detailBase} Current waiters indicate live contention. Historical totals do not identify a current incident.`,
      label: "Pressure" as const,
      tone: "warning" as const,
    };
  }

  return {
    detail: `${detailBase} No current waiters are visible; historical totals do not establish live contention.`,
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
