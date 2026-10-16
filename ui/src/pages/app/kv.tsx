import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import {
  domainResourceInventoryRows,
  type DomainResourceMetricColumn,
} from "@/components/shared/domain-resource-inventory-table";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatNumber } from "@/shared/format";

type AvailableMetricColumn = DomainResourceMetricColumn & { available: boolean };

function formatMaybeNumber(value: number | undefined) {
  return value === undefined ? "--" : formatNumber(value);
}

function formatRecordCount(row: { estimatedRecordCount?: number; estimateComplete?: boolean }) {
  const count = formatMaybeNumber(row.estimatedRecordCount);
  return row.estimateComplete === false && count !== "--" ? `${count}+` : count;
}

function formatStorageBytes(value: number | undefined) {
  if (value === undefined) return "--";
  if (value < 1024) return `${formatNumber(value)} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
}

function formatLatency(value: number | undefined) {
  return value === undefined ? "--" : value.toFixed(1);
}

export default function KvPage() {
  const inventory = createResourceInventoryQuery("kv");
  const inventoryRows = domainResourceInventoryRows(inventory.data);
  const metricCandidates: readonly AvailableMetricColumn[] = [
    {
      id: "records",
      priority: "secondary",
      header: "Records",
      width: "8%",
      cell: formatRecordCount,
      sortValue: (row) => row.estimatedRecordCount,
      title: (row) => (row.estimateComplete === false ? "Estimate incomplete" : undefined),
      available: inventoryRows.some((row) => row.estimatedRecordCount !== undefined),
    },
    {
      id: "storage",
      priority: "secondary",
      header: "Storage",
      width: "8%",
      cell: (row) => formatStorageBytes(row.estimatedStorageBytes),
      sortValue: (row) => row.estimatedStorageBytes,
      available: inventoryRows.some((row) => row.estimatedStorageBytes !== undefined),
    },
    {
      id: "transactions",
      header: "Active transactions",
      width: "12%",
      cell: (row) => formatMaybeNumber(row.transactionsActive),
      sortValue: (row) => row.transactionsActive,
      available: inventoryRows.some((row) => row.transactionsActive !== undefined),
    },
    {
      id: "read-latency",
      rollup: "worst",
      header: "Worst reported read p95 ms",
      width: "12%",
      cell: (row) => formatLatency(row.readLatencyP95Ms),
      sortValue: (row) => row.readLatencyP95Ms,
      available: inventoryRows.some((row) => row.readLatencyP95Ms !== undefined),
    },
    {
      id: "write-latency",
      rollup: "worst",
      header: "Worst reported write p95 ms",
      width: "12%",
      cell: (row) => formatLatency(row.writeLatencyP95Ms),
      sortValue: (row) => row.writeLatencyP95Ms,
      available: inventoryRows.some((row) => row.writeLatencyP95Ms !== undefined),
    },
  ];
  const metricColumns = metricCandidates
    .filter((column) => column.available)
    .map(({ available: _available, ...column }) => column);

  return (
    <DomainInventoryPage
      domain="kv"
      eyebrow="Authoritative state"
      title="KV tables"
      refreshLabel="Refresh KV"
      inventory={inventory}
      loadingDescription="Loading KV tables..."
      errorTitle="Unable to load KV tables"
      refreshingDescription="Refreshing KV tables..."
      emptyDescription="No KV tables are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={metricColumns}
      status={inventory.data ? { label: "Health unavailable", tone: "info" } : undefined}
    />
  );
}
