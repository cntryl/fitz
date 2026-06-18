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

function metricWithPressure(value: number, label: string) {
  return {
    label,
    value,
    ...(value > 0 ? { caption: "attention" } : undefined),
  };
}

function summarizeKvHealth(stats: {
  commitsFailedTotal: number;
  invalidTransactionRejectsTotal: number;
  keysTotal: number;
  rollbacksTotal: number;
  transactionsActive: number;
}) {
  const pressureSignals = [
    stats.commitsFailedTotal > 0 ? `${stats.commitsFailedTotal} commit failure(s)` : null,
    stats.rollbacksTotal > 0 ? `${stats.rollbacksTotal} rollback(s)` : null,
    stats.invalidTransactionRejectsTotal > 0
      ? `${stats.invalidTransactionRejectsTotal} invalid reject(s)`
      : null,
  ].filter((signal): signal is string => signal !== null);

  const hasPressure = pressureSignals.length > 0;

  if (hasPressure) {
    return {
      detail: `${stats.keysTotal} keys are currently authoritative. ${stats.transactionsActive} active transaction session(s) are broker-local. ${pressureSignals.join(", ")} indicate transactional pressure.`,
      label: "Attention" as const,
      tone: "danger" as const,
    };
  }

  return {
    detail: `${stats.keysTotal} keys are currently authoritative with ${stats.transactionsActive} active transaction session(s). Transactions are broker-local and session-scoped.`,
    label: "Live" as const,
    tone: "success" as const,
  };
}

export default function KvPage() {
  const overview = createKvOverviewQuery();
  const inventory = createResourceInventoryQuery("kv");
  const data = overview.data;
  const health = summarizeKvHealth(
    data?.stats ?? {
      commitsFailedTotal: 0,
      invalidTransactionRejectsTotal: 0,
      keysTotal: 0,
      rollbacksTotal: 0,
      transactionsActive: 0,
    },
  );
  const sidebar = createDomainSidebar({
    data,
    title: "KV snapshot",
    description: "Current authoritative state and transaction context.",
    stats: (current) => [
      { label: "Keys total", value: current.stats.keysTotal },
      {
        label: "Active transactions",
        value: current.stats.transactionsActive,
        note: "Broker-local/session-scoped",
      },
      {
        label: "Transaction pressure",
        value:
          current.stats.commitsFailedTotal +
          current.stats.rollbacksTotal +
          current.stats.invalidTransactionRejectsTotal,
      },
      {
        label: "Ops / sec",
        value: current.stats.operationsPerSecond.toFixed(2),
        note: "Live broker sample",
      },
    ],
  });

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Authoritative state"
          title="KV overview"
          description="Current authoritative KV state and transaction pressure by realm."
          primaryAction={{
            label: "Refresh KV",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: health.detail,
            label: overview.refreshing ? "Refreshing" : overview.stale ? "Stale" : health.label,
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : health.tone,
          }}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading KV overview..." />
        ) : null}

        {!data && overview.error ? (
          <QueryErrorState
            title="Unable to load KV overview"
            error={overview.error}
            onRetry={() => overview.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing KV overview..." />
            ) : null}

            <DomainMetricTable
              title="KV metrics"
              description="Current key count, transaction pressure, and throughput."
              metrics={[
                { label: "Keys total", value: data.stats.keysTotal },
                { label: "Active transactions", value: data.stats.transactionsActive },
                { label: "Ops / sec", value: data.stats.operationsPerSecond.toFixed(2) },
                metricWithPressure(data.stats.commitsFailedTotal, "Commit failures"),
                metricWithPressure(data.stats.rollbacksTotal, "Rollbacks"),
                metricWithPressure(
                  data.stats.invalidTransactionRejectsTotal,
                  "Invalid transaction rejects",
                ),
              ]}
            />

            <DomainBarChart
              title="KV signal"
              description="Current key volume and live transaction pressure."
              label="KV state snapshot"
              scope="Live KV snapshot"
              data={[
                {
                  label: "Active transactions",
                  unitLabel: "transactions",
                  value: data.stats.transactionsActive,
                },
                {
                  label: "Keys",
                  unitLabel: "keys",
                  value: data.stats.keysTotal,
                },
                {
                  label: "Ops / sec",
                  unitLabel: "ops/sec",
                  value: data.stats.operationsPerSecond,
                },
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
