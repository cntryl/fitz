import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import type { DomainResourceMetricColumn } from "@/components/shared/domain-resource-inventory-table";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatBytes, formatNumber } from "@/shared/format";

function maybeCount(value: number | undefined) {
  return value === undefined ? "--" : formatNumber(value);
}

export default function StreamPage() {
  const inventory = createResourceInventoryQuery("stream");
  const streamMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "committed",
      header: "Committed",
      width: "14%",
      cell: (row) => maybeCount(row.committedEventCount),
      sortValue: (row) => row.committedEventCount,
    },
    {
      id: "subscriptions",
      header: "Live subscriptions",
      width: "18%",
      cell: (row) => maybeCount(row.subscriptionsActive),
      sortValue: (row) => row.subscriptionsActive,
    },
    {
      id: "storage",
      priority: "secondary",
      header: "Storage",
      width: "14%",
      cell: (row) => (row.sizeBytes === undefined ? "--" : formatBytes(row.sizeBytes)),
      sortValue: (row) => row.sizeBytes,
    },
    {
      id: "sessions",
      priority: "secondary",
      header: "Append sessions",
      width: "18%",
      cell: (row) => maybeCount(row.sessionsActive),
      sortValue: (row) => row.sessionsActive,
    },
  ];

  return (
    <DomainInventoryPage
      domain="stream"
      eyebrow="Durable history"
      title="Stream inventory"
      refreshLabel="Refresh stream"
      inventory={inventory}
      loadingDescription="Loading stream inventory..."
      errorTitle="Unable to load stream inventory"
      refreshingDescription="Refreshing stream inventory..."
      emptyDescription="No stream resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={streamMetricColumns}
      status={inventory.data ? { label: "Consumer health unavailable", tone: "info" } : undefined}
    />
  );
}
