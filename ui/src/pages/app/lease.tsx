import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import {
  domainResourceInventoryRows,
  type DomainResourceMetricColumn,
} from "@/components/shared/domain-resource-inventory-table";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatDurationSeconds, formatNumber } from "@/shared/format";

function maybeCount(value: number | undefined) {
  return value === undefined ? "--" : formatNumber(value);
}

export default function LeasePage() {
  const inventory = createResourceInventoryQuery("lease");
  const rows = domainResourceInventoryRows(inventory.data);
  const hasWaiters = rows.some((row) => (row.waiters ?? 0) > 0);
  const leaseMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "waiters",
      header: "Waiters",
      width: "14%",
      cell: (row) => maybeCount(row.waiters),
      sortValue: (row) => row.waiters,
    },
    {
      id: "active",
      priority: "secondary",
      header: "Active leases",
      width: "14%",
      cell: (row) => maybeCount(row.activeLeases),
      sortValue: (row) => row.activeLeases,
    },
    {
      id: "oldest",
      priority: "secondary",
      rollup: "worst",
      header: "Oldest ownership",
      width: "18%",
      cell: (row) =>
        row.oldestLeaseAgeSeconds === undefined
          ? "--"
          : formatDurationSeconds(row.oldestLeaseAgeSeconds),
      sortValue: (row) => row.oldestLeaseAgeSeconds,
      title: () => "Long ownership can be expected when a lease is renewed.",
    },
  ];

  return (
    <DomainInventoryPage
      domain="lease"
      eyebrow="Ownership coordination"
      title="Lease inventory"
      refreshLabel="Refresh lease"
      inventory={inventory}
      loadingDescription="Loading lease inventory..."
      errorTitle="Unable to load lease inventory"
      refreshingDescription="Refreshing lease inventory..."
      emptyDescription="No lease resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={leaseMetricColumns}
      status={
        inventory.data
          ? {
              label: hasWaiters ? "Waiters present" : "Health unavailable",
              tone: hasWaiters ? "warning" : "info",
            }
          : undefined
      }
    />
  );
}
