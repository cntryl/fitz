import DomainHeader from "@/components/shared/domain-header";
import DomainBarChart from "@/components/shared/domain-bar-chart";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { Stack } from "@askrjs/themes/layouts";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createKvOverviewQuery } from "@/features/kv/kv-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

export default function KvPage() {
  const overview = createKvOverviewQuery();
  const inventory = createResourceInventoryQuery("kv");
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "KV snapshot",
    description: "Current key-value broker state and realm inventory.",
    stats: (current) => [
      { label: "Keys", value: current.stats.keysTotal },
      { label: "Transactions", value: current.stats.transactionsActive },
      {
        label: "Ops / sec",
        value: current.stats.operationsPerSecond.toFixed(2),
        note: "Live broker snapshot",
      },
    ],
  });

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          domain="KV"
          title="KV overview"
          description="Key-value broker statistics and live realm inventory."
          onRefresh={() => overview.refresh()}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading KV overview..." />
        ) : null}

        {!data && overview.error ? <QueryErrorState error={overview.error} /> : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing KV overview..." />
            ) : null}

            <DomainMetricTable
              title="KV metrics"
              metrics={[
                { label: "Keys", value: data.stats.keysTotal },
                { label: "Transactions", value: data.stats.transactionsActive },
                { label: "Commit fails", value: data.stats.commitsFailedTotal },
                { label: "Rollbacks", value: data.stats.rollbacksTotal },
                { label: "Txn rejects", value: data.stats.invalidTransactionRejectsTotal },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
              ]}
            />

            <DomainBarChart
              title="KV signal"
              description="Current key volume, transactional pressure, and throughput."
              label="KV state snapshot"
              data={[
                ["Keys", data.stats.keysTotal],
                ["Transactions", data.stats.transactionsActive],
                ["Ops / sec", data.stats.operationsPerSecond],
              ]}
            />

            <DomainRealmTable
              title="KV realms"
              realms={data.realms}
              emptyMessage="No KV realms are currently visible."
            />

            <DomainResourceBrowser
              domain="kv"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
