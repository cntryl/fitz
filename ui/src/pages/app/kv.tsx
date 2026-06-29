import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import type { DomainResourceMetricColumn } from "@/components/shared/domain-resource-inventory-table";
import { createKvOverviewQuery } from "@/features/kv/kv-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatNumber } from "@/shared/format";

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

function resourceCount(data: ReturnType<typeof createResourceInventoryQuery>["data"]) {
  return (
    data?.realms.reduce(
      (sum, realm) =>
        sum +
        realm.areas.reduce(
          (areaSum, area) =>
            areaSum +
            ((area.resourceEntries?.length ?? 0) > 0
              ? area.resourceEntries.length
              : area.resources.length),
          0,
        ),
      0,
    ) ?? 0
  );
}

export default function KvPage() {
  const overview = createKvOverviewQuery();
  const inventory = createResourceInventoryQuery("kv");
  const tableCount = resourceCount(inventory.data);
  const stats = overview.data?.stats;
  const kvMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "records",
      header: "Records",
      width: "8%",
      cell: formatRecordCount,
      title: (row) => (row.estimateComplete === false ? "Estimate incomplete" : undefined),
    },
    {
      id: "storage",
      header: "Storage",
      width: "8%",
      cell: (row) => formatStorageBytes(row.estimatedStorageBytes),
    },
    {
      id: "transactions",
      header: "Txns",
      width: "8%",
      cell: (row) => formatMaybeNumber(row.transactionsActive),
    },
    {
      id: "read-latency",
      header: "Read p95 ms",
      width: "9%",
      cell: (row) => formatLatency(row.readLatencyP95Ms),
    },
    {
      id: "write-latency",
      header: "Write p95 ms",
      width: "9%",
      cell: (row) => formatLatency(row.writeLatencyP95Ms),
    },
    {
      id: "domain-keys",
      header: "Domain keys",
      width: "9%",
      cell: () => formatMaybeNumber(stats?.keysTotal),
    },
    {
      id: "domain-txns",
      header: "Domain txns",
      width: "9%",
      cell: () => formatMaybeNumber(stats?.transactionsActive),
    },
    {
      id: "domain-ops",
      header: "Ops / sec",
      width: "9%",
      cell: () => (stats ? stats.operationsPerSecond.toFixed(2) : "--"),
    },
    {
      id: "domain-failures",
      header: "Failures",
      width: "9%",
      cell: () =>
        stats
          ? formatNumber(stats.commitsFailedTotal + stats.invalidTransactionRejectsTotal)
          : "--",
    },
  ];

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
      emptyDescription="No KV tables are currently visible."
      tableTitle="Resource inventory"
      metricColumns={kvMetricColumns}
      status={{
        detail: inventory.data
          ? `${formatNumber(tableCount)} table${tableCount === 1 ? "" : "s"} visible.`
          : "Loading KV table inventory.",
        label: inventory.refreshing ? "Refreshing" : inventory.data ? "Live" : "Loading",
        tone: inventory.refreshing ? "info" : inventory.data ? "success" : "info",
      }}
    />
  );
}
