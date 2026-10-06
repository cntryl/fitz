import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import {
  domainResourceInventoryRows,
  type DomainResourceMetricColumn,
} from "@/components/shared/domain-resource-inventory-table";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatNumber, formatTimestamp } from "@/shared/format";

function maybeCount(value: number | undefined) {
  return value === undefined ? "--" : formatNumber(value);
}

export default function SchedulePage() {
  const inventory = createResourceInventoryQuery("schedule");
  const rows = domainResourceInventoryRows(inventory.data);
  const hasPendingClaims = rows.some((row) => (row.pendingClaims ?? 0) > 0);
  const scheduleMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "pending-claims",
      header: "Pending claims",
      width: "16%",
      cell: (row) => maybeCount(row.pendingClaims),
      sortValue: (row) => row.pendingClaims,
    },
    {
      id: "next-run",
      rollup: "earliest",
      header: "Next run",
      width: "20%",
      cell: (row) => (row.nextRun ? formatTimestamp(row.nextRun) : "--"),
      sortValue: (row) => {
        const parsed = row.nextRun ? Date.parse(row.nextRun) : Number.NaN;
        return Number.isNaN(parsed) ? null : parsed;
      },
    },
    {
      id: "enabled",
      priority: "secondary",
      header: "Enabled schedules",
      width: "14%",
      cell: (row) => maybeCount(row.schedulesActive),
      sortValue: (row) => row.schedulesActive,
    },
  ];

  return (
    <DomainInventoryPage
      domain="schedule"
      eyebrow="Timing intent"
      title="Schedule inventory"
      refreshLabel="Refresh schedule"
      inventory={inventory}
      loadingDescription="Loading schedule inventory..."
      errorTitle="Unable to load schedule inventory"
      refreshingDescription="Refreshing schedule inventory..."
      emptyDescription="No schedule resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={scheduleMetricColumns}
      status={
        inventory.data
          ? {
              label: hasPendingClaims ? "Claims pending" : "Health unavailable",
              tone: hasPendingClaims ? "warning" : "info",
            }
          : undefined
      }
    />
  );
}
