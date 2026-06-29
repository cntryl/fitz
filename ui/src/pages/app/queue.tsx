import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import type { DomainResourceMetricColumn } from "@/components/shared/domain-resource-inventory-table";
import { createQueueInventoryQuery, createQueueOverviewQuery } from "@/features/queue/queue-query";
import type { QueueStatsSummary } from "@/features/queue/queue-models";
import { formatDurationSeconds, formatNumber } from "@/shared/format";

function queueVisibleCount(stats: QueueStatsSummary) {
  return (
    stats.messagesReady + stats.messagesDelayed + stats.inflightActive + stats.messagesDeadLettered
  );
}

function describeQueueStats(stats: QueueStatsSummary) {
  const visible = queueVisibleCount(stats);

  if (stats.messagesDeadLettered > 0) {
    return `${formatNumber(stats.messagesDeadLettered)} dead-lettered message(s) need explicit operator action.`;
  }

  if (stats.messagesReady > 0 || stats.messagesDelayed > 0) {
    return `${formatNumber(visible)} message(s) are visible across ready, delayed, inflight, and dead-letter states. Oldest backlog is ${formatDurationSeconds(
      stats.oldestBacklogAgeSeconds,
    )}.`;
  }

  if (stats.inflightActive > 0) {
    return `${formatNumber(stats.inflightActive)} message(s) are currently in flight.`;
  }

  return "No visible queue backlog at this level.";
}

function queueStatus(stats: QueueStatsSummary) {
  if (stats.messagesDeadLettered > 0) {
    return { label: "Attention" as const, tone: "danger" as const };
  }

  if (queueVisibleCount(stats) > 0) {
    return { label: "Pressure" as const, tone: "warning" as const };
  }

  return { label: "Live" as const, tone: "success" as const };
}

function resourceCount(data: ReturnType<typeof createQueueInventoryQuery>["data"]) {
  return (
    data?.realms.reduce(
      (sum, realm) =>
        sum + realm.areas.reduce((areaSum, area) => areaSum + area.resources.length, 0),
      0,
    ) ?? 0
  );
}

export default function QueuePage() {
  const overview = createQueueOverviewQuery();
  const inventory = createQueueInventoryQuery();
  const currentStatus = overview.data ? queueStatus(overview.data.stats) : null;
  const queueCount = resourceCount(inventory.data);
  const stats = overview.data?.stats;
  const queueMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "ready",
      header: "Ready",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.messagesReady) : "--"),
    },
    {
      id: "delayed",
      header: "Delayed",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.messagesDelayed) : "--"),
    },
    {
      id: "inflight",
      header: "In flight",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.inflightActive) : "--"),
    },
    {
      id: "dead-lettered",
      header: "Dead-lettered",
      width: "12%",
      cell: () => (stats ? formatNumber(stats.messagesDeadLettered) : "--"),
    },
    {
      id: "oldest",
      header: "Oldest",
      width: "10%",
      cell: () => (stats ? formatDurationSeconds(stats.oldestBacklogAgeSeconds) : "--"),
    },
  ];

  return (
    <DomainInventoryPage
      domain="queue"
      eyebrow="Durable work"
      title="Queue inventory"
      description="Durable work resources for the active route family."
      refreshLabel="Refresh queue"
      inventory={inventory}
      refreshing={overview.refreshing || inventory.refreshing}
      refreshers={[() => overview.refresh(), () => inventory.refresh()]}
      loadingDescription="Loading queue inventory..."
      errorTitle="Unable to load queue inventory"
      refreshingDescription="Refreshing queue inventory..."
      emptyDescription="No queue resources are currently visible."
      tableTitle="Resource inventory"
      metricColumns={queueMetricColumns}
      status={{
        detail: overview.data
          ? `${formatNumber(queueCount)} queue${queueCount === 1 ? "" : "s"} visible. ${describeQueueStats(
              overview.data.stats,
            )}`
          : overview.error
            ? "Queue health is unavailable. Resource inventory can still be inspected when loaded."
            : "Loading queue health.",
        label: overview.refreshing
          ? "Refreshing"
          : overview.error
            ? "Health unavailable"
            : overview.stale
              ? "Stale"
              : (currentStatus?.label ?? "Loading"),
        tone: overview.refreshing
          ? "info"
          : overview.error
            ? "warning"
            : overview.stale
              ? "warning"
              : (currentStatus?.tone ?? "info"),
      }}
    />
  );
}
