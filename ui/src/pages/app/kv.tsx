import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import {
  domainResourceInventoryRows,
  type DomainResourceMetricColumn,
} from "@/components/shared/domain-resource-inventory-table";
import { createKvOverviewQuery } from "@/features/kv/kv-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatNumber } from "@/shared/format";

type KvOverviewStats = NonNullable<ReturnType<typeof createKvOverviewQuery>["data"]>["stats"];
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

function kvCumulativeFailureCount(stats: KvOverviewStats) {
  return stats.commitsFailedTotal + stats.invalidTransactionRejectsTotal;
}

function describeKvActivity(stats: KvOverviewStats) {
  return `${formatNumber(stats.keysTotal)} keys, ${formatNumber(
    stats.transactionsActive,
  )} active ${stats.transactionsActive === 1 ? "transaction" : "transactions"}, ${stats.operationsPerSecond.toFixed(2)} ops/sec. Cumulative process totals include ${formatNumber(
    kvCumulativeFailureCount(stats),
  )} failure ${kvCumulativeFailureCount(stats) === 1 ? "signal" : "signals"}; historical totals do not establish current pressure.`;
}

export default function KvPage() {
  const overview = createKvOverviewQuery();
  const inventory = createResourceInventoryQuery("kv");
  const stats = overview.data?.stats;
  const inventoryRows = domainResourceInventoryRows(inventory.data);
  const kvHealth = stats
    ? ({
        label: stats.transactionsActive > 0 || stats.operationsPerSecond > 0 ? "Active" : "Live",
        tone: stats.transactionsActive > 0 || stats.operationsPerSecond > 0 ? "info" : "success",
      } as const)
    : null;
  const metricCandidates: readonly AvailableMetricColumn[] = [
    {
      id: "records",
      header: "Records",
      width: "8%",
      cell: formatRecordCount,
      sortValue: (row) => row.estimatedRecordCount,
      title: (row) => (row.estimateComplete === false ? "Estimate incomplete" : undefined),
      available: inventoryRows.some((row) => row.estimatedRecordCount !== undefined),
    },
    {
      id: "storage",
      header: "Storage",
      width: "8%",
      cell: (row) => formatStorageBytes(row.estimatedStorageBytes),
      sortValue: (row) => row.estimatedStorageBytes,
      available: inventoryRows.some((row) => row.estimatedStorageBytes !== undefined),
    },
    {
      id: "transactions",
      header: "Txns",
      width: "8%",
      cell: (row) => formatMaybeNumber(row.transactionsActive),
      sortValue: (row) => row.transactionsActive,
      available: inventoryRows.some((row) => row.transactionsActive !== undefined),
    },
    {
      id: "read-latency",
      header: "Read p95 ms",
      width: "9%",
      cell: (row) => formatLatency(row.readLatencyP95Ms),
      sortValue: (row) => row.readLatencyP95Ms,
      available: inventoryRows.some((row) => row.readLatencyP95Ms !== undefined),
    },
    {
      id: "write-latency",
      header: "Write p95 ms",
      width: "9%",
      cell: (row) => formatLatency(row.writeLatencyP95Ms),
      sortValue: (row) => row.writeLatencyP95Ms,
      available: inventoryRows.some((row) => row.writeLatencyP95Ms !== undefined),
    },
  ];
  const kvMetricColumns = metricCandidates
    .filter((column) => column.available)
    .map(({ available: _available, ...column }) => column);

  return (
    <DomainInventoryPage
      domain="kv"
      eyebrow="Authoritative state"
      title="KV tables"
      description="Authoritative KV tables for the active route family."
      refreshLabel="Refresh KV"
      inventory={inventory}
      refreshing={overview.refreshing || inventory.refreshing}
      refreshers={[() => overview.refresh(), () => inventory.refresh()]}
      loadingDescription="Loading KV tables..."
      errorTitle="Unable to load KV tables"
      refreshingDescription="Refreshing KV tables..."
      emptyDescription="No KV tables are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      metricColumns={kvMetricColumns}
      stats={[
        { label: "Domain keys", value: stats ? formatNumber(stats.keysTotal) : "--" },
        {
          label: "Active txns",
          value: stats ? formatNumber(stats.transactionsActive) : "--",
        },
        { label: "Ops / sec", value: stats ? stats.operationsPerSecond.toFixed(2) : "--" },
      ]}
      status={{
        detail: stats
          ? `Current authoritative-state activity: ${describeKvActivity(stats)}`
          : overview.error
            ? "KV health is unavailable. Resource inventory can still be inspected when loaded."
            : "Loading KV health.",
        label: overview.refreshing
          ? "Refreshing"
          : overview.error
            ? "Health unavailable"
            : overview.stale
              ? "Stale"
              : (kvHealth?.label ?? "Loading"),
        tone: overview.refreshing
          ? "info"
          : overview.error
            ? "warning"
            : overview.stale
              ? "warning"
              : (kvHealth?.tone ?? "info"),
      }}
    />
  );
}
