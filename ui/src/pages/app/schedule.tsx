import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import { queryHeaderStatus } from "@/components/shared/query-header-status";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { createScheduleOverviewQuery } from "@/features/schedule/schedule-query";
import type { ScheduleOverview } from "@/features/schedule/schedule-models";
import { formatCount, formatNumber } from "@/shared/format";

interface ScheduleHealth {
  detail: string;
  label: "Ready" | "Pressure" | "Attention";
  tone: "success" | "warning" | "danger";
}

function summarizeScheduleHealth(stats: ScheduleOverview["stats"]): ScheduleHealth {
  const persistenceFailures =
    stats.createPersistenceFailuresTotal +
    stats.upsertPersistenceFailuresTotal +
    stats.cancelPersistenceFailuresTotal;
  const handoffFailures =
    stats.ackFailuresTotal + stats.notifyFailuresTotal + stats.overdueNormalizationsTotal;
  const failures = persistenceFailures + handoffFailures;
  const baseDetail = `${formatCount(stats.schedulesActive, "active schedule")}, ${formatCount(
    stats.pendingFireClaims,
    "pending fire claim",
  )}, ${formatCount(stats.executionsPerMinute, "acknowledged handoff")}/min, ${formatCount(
    failures,
    "cumulative failure counter",
  )}.`;

  if (stats.pendingFireClaims > 0) {
    return {
      detail: `${baseDetail} Pending fire claims are waiting for live handoff. Cumulative failures describe process history, not a current incident.`,
      label: "Pressure",
      tone: "warning",
    };
  }

  return {
    detail:
      stats.subscriptionsActive > 0
        ? `${baseDetail} ${formatNumber(
            stats.subscriptionsActive,
          )} schedule ${stats.subscriptionsActive === 1 ? "subscription is" : "subscriptions are"} currently visible; downstream handling is not reported. Cumulative failures describe process history, not a current incident.`
        : `${baseDetail} No schedule subscriptions are currently visible; this does not prove downstream availability. Cumulative failures describe process history, not a current incident.`,
    label: "Ready",
    tone: "success",
  };
}

export default function SchedulePage() {
  const overview = createScheduleOverviewQuery();
  const inventory = createResourceInventoryQuery("schedule");
  const emptyStats: ScheduleOverview["stats"] = {
    ackFailuresTotal: 0,
    cancelPersistenceFailuresTotal: 0,
    createPersistenceFailuresTotal: 0,
    executionsPerMinute: 0,
    notifyFailuresTotal: 0,
    overdueNormalizationsTotal: 0,
    pendingFireClaims: 0,
    schedulesActive: 0,
    subscriptionsActive: 0,
    upsertPersistenceFailuresTotal: 0,
  };
  const health = summarizeScheduleHealth(overview.data?.stats ?? emptyStats);
  const stats = overview.data?.stats;
  return (
    <DomainInventoryPage
      domain="schedule"
      eyebrow="Timing intent"
      title="Schedule inventory"
      description="Durable timing resources for the active Route Family."
      refreshLabel="Refresh schedule"
      inventory={inventory}
      refreshing={overview.refreshing || inventory.refreshing}
      refreshers={[() => overview.refresh(), () => inventory.refresh()]}
      loadingDescription="Loading schedule inventory..."
      errorTitle="Unable to load schedule inventory"
      refreshingDescription="Refreshing schedule inventory..."
      emptyDescription="No schedule resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      stats={[
        { label: "Active", value: stats ? formatNumber(stats.schedulesActive) : "--" },
        { label: "Pending claims", value: stats ? formatNumber(stats.pendingFireClaims) : "--" },
        {
          label: "Handoffs / min",
          value: stats ? stats.executionsPerMinute.toFixed(2) : "--",
        },
      ]}
      status={queryHeaderStatus(
        overview,
        {
          loading: "Loading schedule health.",
          ready: `${health.detail} Schedule does not imply durable downstream delivery.`,
          unavailable:
            "Schedule health is unavailable. Resource inventory can still be inspected when loaded.",
        },
        { label: health.label, tone: health.tone },
      )}
    />
  );
}
