import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import type { DomainResourceMetricColumn } from "@/components/shared/domain-resource-inventory-table";
import { createQueueInventoryQuery, createQueueOverviewQuery } from "@/features/queue/queue-query";
import type { QueueStatsSummary } from "@/features/queue/queue-models";
import { formatCount, formatDurationSeconds, formatNumber } from "@/shared/format";

function queueVisibleCount(stats: QueueStatsSummary) {
  return (
    stats.messagesReady + stats.messagesDelayed + stats.inflightActive + stats.messagesDeadLettered
  );
}

function describeQueueStats(stats: QueueStatsSummary) {
  const visible = queueVisibleCount(stats);
  const counts = `Ready ${formatNumber(stats.messagesReady)}, delayed ${formatNumber(
    stats.messagesDelayed,
  )}, inflight ${formatNumber(stats.inflightActive)}, dead letters ${formatNumber(
    stats.messagesDeadLettered,
  )}. Oldest backlog ${formatDurationSeconds(stats.oldestBacklogAgeSeconds)}.`;

  if (stats.messagesDeadLettered > 0) {
    return `${counts} ${formatNumber(stats.messagesDeadLettered)} dead-lettered ${
      stats.messagesDeadLettered === 1 ? "message needs" : "messages need"
    } explicit operator action.`;
  }

  if (stats.messagesReady > 0 || stats.messagesDelayed > 0) {
    return `${counts} ${formatNumber(visible)} ${
      visible === 1 ? "message is" : "messages are"
    } visible across ready, delayed, inflight, and dead-letter states. Activity alone does not establish pressure.`;
  }

  if (stats.inflightActive > 0) {
    return `${counts} ${formatNumber(stats.inflightActive)} live ${
      stats.inflightActive === 1 ? "reservation is" : "reservations are"
    } currently in flight.`;
  }

  return `${counts} No durable queue backlog is visible at this level.`;
}

function queueStatus(stats: QueueStatsSummary) {
  if (stats.messagesDeadLettered > 0) {
    return {
      label: "Attention" as const,
      reason: `${formatCount(stats.messagesDeadLettered, "message is", "messages are")} dead-lettered and need inspection or replay. Sort realms by Dead-lettered to find them.`,
      tone: "danger" as const,
    };
  }

  if (queueVisibleCount(stats) > 0) {
    return {
      label: "Active" as const,
      reason: `Work is flowing with no dead letters; the oldest backlog has waited ${formatDurationSeconds(
        stats.oldestBacklogAgeSeconds,
      )}.`,
      tone: "info" as const,
    };
  }

  return {
    label: "Healthy" as const,
    reason: "No queued work is waiting.",
    tone: "success" as const,
  };
}

export default function QueuePage() {
  const overview = createQueueOverviewQuery();
  const inventory = createQueueInventoryQuery();
  const currentStatus = overview.data ? queueStatus(overview.data.stats) : null;
  const stats = overview.data?.stats;
  const queueMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "ready",
      header: "Ready",
      width: "10%",
      cell: (row) => formatNumber(row.messagesReady ?? 0),
      sortValue: (row) => row.messagesReady,
    },
    {
      id: "delayed",
      priority: "secondary",
      header: "Delayed",
      width: "10%",
      cell: (row) => formatNumber(row.messagesDelayed ?? 0),
      sortValue: (row) => row.messagesDelayed,
    },
    {
      id: "inflight",
      priority: "secondary",
      header: "In flight",
      width: "10%",
      cell: (row) => formatNumber(row.messagesInflight ?? 0),
      sortValue: (row) => row.messagesInflight,
    },
    {
      id: "dead-lettered",
      header: "Dead-lettered",
      width: "12%",
      cell: (row) => formatNumber(row.messagesDeadLettered ?? 0),
      sortValue: (row) => row.messagesDeadLettered,
    },
    {
      id: "oldest",
      rollup: "worst",
      header: "Oldest",
      width: "10%",
      cell: (row) => formatDurationSeconds(row.oldestBacklogAgeSeconds ?? 0),
      sortValue: (row) => row.oldestBacklogAgeSeconds,
    },
    {
      id: "subscriptions",
      priority: "secondary",
      header: "Active subscriptions",
      width: "16%",
      cell: (row) => formatNumber(row.subscriptionsActive ?? 0),
      sortValue: (row) => row.subscriptionsActive,
      title: () => "Current live subscriptions matching this queue route.",
    },
  ];

  return (
    <DomainInventoryPage
      domain="queue"
      eyebrow="Durable work"
      title="Queue inventory"
      description="Durable work resources for the active Route Family."
      refreshLabel="Refresh queue"
      inventory={inventory}
      refreshing={overview.refreshing || inventory.refreshing}
      refreshers={[() => overview.refresh(), () => inventory.refresh()]}
      loadingDescription="Loading queue inventory..."
      errorTitle="Unable to load queue inventory"
      refreshingDescription="Refreshing queue inventory..."
      emptyDescription="No queue resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={queueMetricColumns}
      reason={currentStatus?.reason}
      stats={[
        { label: "Ready", value: stats ? formatNumber(stats.messagesReady) : "--" },
        { label: "In flight", value: stats ? formatNumber(stats.inflightActive) : "--" },
        { label: "Dead letters", value: stats ? formatNumber(stats.messagesDeadLettered) : "--" },
      ]}
      status={{
        detail: overview.data
          ? describeQueueStats(overview.data.stats)
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
