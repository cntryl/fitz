import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import type { DomainResourceMetricColumn } from "@/components/shared/domain-resource-inventory-table";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { createScheduleOverviewQuery } from "@/features/schedule/schedule-query";
import type { ScheduleOverview } from "@/features/schedule/schedule-models";
import { formatNumber } from "@/shared/format";

interface ScheduleHealth {
  detail: string;
  label: "Live" | "Pressure" | "Attention";
  tone: "success" | "warning" | "danger";
}

function summarizeScheduleHealth(stats: ScheduleOverview["stats"]): ScheduleHealth {
  const persistenceFailures =
    stats.createPersistenceFailuresTotal +
    stats.upsertPersistenceFailuresTotal +
    stats.cancelPersistenceFailuresTotal;
  const handoffFailures =
    stats.ackFailuresTotal + stats.notifyFailuresTotal + stats.overdueNormalizationsTotal;

  if (persistenceFailures > 0 || handoffFailures > 0) {
    return {
      detail: `${formatNumber(stats.schedulesActive)} active schedules are visible. Persistence and handoff failure counters need attention.`,
      label: "Attention",
      tone: "danger",
    };
  }

  if (stats.pendingFireClaims > 0) {
    return {
      detail: `${formatNumber(stats.pendingFireClaims)} pending fire claim(s) are waiting for live handoff.`,
      label: "Pressure",
      tone: "warning",
    };
  }

  return {
    detail:
      stats.subscriptionsActive > 0
        ? `${formatNumber(stats.subscriptionsActive)} active live subscription(s) are visible for handoff.`
        : "No live handoff subscriptions are visible.",
    label: "Live",
    tone: "success",
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

function failureCount(stats: ScheduleOverview["stats"]) {
  return (
    stats.createPersistenceFailuresTotal +
    stats.upsertPersistenceFailuresTotal +
    stats.cancelPersistenceFailuresTotal +
    stats.ackFailuresTotal +
    stats.notifyFailuresTotal +
    stats.overdueNormalizationsTotal
  );
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
  const scheduleCount = resourceCount(inventory.data);
  const stats = overview.data?.stats;
  const scheduleMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "active",
      header: "Active",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.schedulesActive) : "--"),
    },
    {
      id: "subscriptions",
      header: "Subscriptions",
      width: "12%",
      cell: () => (stats ? formatNumber(stats.subscriptionsActive) : "--"),
    },
    {
      id: "pending-claims",
      header: "Pending claims",
      width: "13%",
      cell: () => (stats ? formatNumber(stats.pendingFireClaims) : "--"),
    },
    {
      id: "failures",
      header: "Failures",
      width: "10%",
      cell: () => (stats ? formatNumber(failureCount(stats)) : "--"),
    },
    {
      id: "executions",
      header: "Exec / min",
      width: "10%",
      cell: () => (stats ? stats.executionsPerMinute.toFixed(2) : "--"),
    },
  ];

  return (
    <DomainInventoryPage
      domain="schedule"
      eyebrow="Timing intent"
      title="Schedule inventory"
      description="Durable timing resources for the active route family."
      refreshLabel="Refresh schedule"
      inventory={inventory}
      refreshing={overview.refreshing || inventory.refreshing}
      refreshers={[() => overview.refresh(), () => inventory.refresh()]}
      loadingDescription="Loading schedule inventory..."
      errorTitle="Unable to load schedule inventory"
      refreshingDescription="Refreshing schedule inventory..."
      emptyDescription="No schedule resources are currently visible."
      tableTitle="Resource inventory"
      metricColumns={scheduleMetricColumns}
      status={{
        detail: overview.data
          ? `${formatNumber(scheduleCount)} schedule resource${
              scheduleCount === 1 ? "" : "s"
            } visible. ${health.detail} Schedule does not imply durable downstream delivery.`
          : overview.error
            ? "Schedule health is unavailable. Resource inventory can still be inspected when loaded."
            : "Loading schedule health.",
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
