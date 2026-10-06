import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import {
  domainResourceInventoryRows,
  type DomainResourceMetricColumn,
} from "@/components/shared/domain-resource-inventory-table";
import { createQueueInventoryQuery } from "@/features/queue/queue-query";
import { formatNumber } from "@/shared/format";

function maybeCount(value: number | undefined) {
  return value === undefined ? "--" : formatNumber(value);
}

export default function QueuePage() {
  const inventory = createQueueInventoryQuery();
  const rows = domainResourceInventoryRows(inventory.data);
  const hasDeadLetters = rows.some((row) => (row.messagesDeadLettered ?? 0) > 0);
  const queueMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "ready",
      header: "Ready",
      width: "10%",
      cell: (row) => maybeCount(row.messagesReady),
      sortValue: (row) => row.messagesReady,
    },
    {
      id: "dead-lettered",
      header: "Dead-lettered",
      width: "12%",
      cell: (row) => maybeCount(row.messagesDeadLettered),
      sortValue: (row) => row.messagesDeadLettered,
    },
    {
      id: "delayed",
      priority: "secondary",
      header: "Delayed",
      width: "10%",
      cell: (row) => maybeCount(row.messagesDelayed),
      sortValue: (row) => row.messagesDelayed,
    },
    {
      id: "inflight",
      priority: "secondary",
      header: "In flight",
      width: "10%",
      cell: (row) => maybeCount(row.messagesInflight),
      sortValue: (row) => row.messagesInflight,
    },
    {
      id: "subscriptions",
      priority: "secondary",
      header: "Live subscriptions",
      width: "16%",
      cell: (row) => maybeCount(row.subscriptionsActive),
      sortValue: (row) => row.subscriptionsActive,
      title: () => "Live route registrations; these do not prove consumer capacity.",
    },
  ];

  return (
    <DomainInventoryPage
      domain="queue"
      eyebrow="Durable work"
      title="Queue inventory"
      refreshLabel="Refresh queue"
      inventory={inventory}
      loadingDescription="Loading queue inventory..."
      errorTitle="Unable to load queue inventory"
      refreshingDescription="Refreshing queue inventory..."
      emptyDescription="No queue resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={queueMetricColumns}
      status={
        inventory.data
          ? {
              label: hasDeadLetters ? "Dead letters" : "Progress unavailable",
              tone: hasDeadLetters ? "warning" : "info",
            }
          : undefined
      }
    />
  );
}
