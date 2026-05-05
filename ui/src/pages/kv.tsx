import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import DomainState from "@/components/shared/domain-state";
import { createKvOverviewQuery } from "@/features/kv/kv-query";

export default function KvPage() {
  const overview = createKvOverviewQuery();
  const data = overview.data;

  return (
    <section class="domain-page">
      <DomainHeader
        domain="KV"
        title="KV overview"
        description="Key-value broker statistics and live realm inventory."
        onRefresh={() => overview.refresh()}
      />

      {overview.loading ? <DomainState kind="loading" message="Loading KV overview..." /> : null}

      {overview.error ? (
        <DomainState
          kind="error"
          message="KV overview could not be loaded."
          error={overview.error}
        />
      ) : null}

      {data && !overview.loading && !overview.error ? (
        <>
          <DomainMetricTable
            title="KV metrics"
            metrics={[
              { label: "Keys", value: data.stats.keysTotal },
              { label: "Transactions", value: data.stats.transactionsActive },
              {
                label: "Ops / sec",
                value: data.stats.operationsPerSecond.toFixed(2),
              },
            ]}
          />

          <DomainRealmTable
            title="KV realms"
            realms={data.realms}
            emptyMessage="No KV realms are currently visible."
          />
        </>
      ) : null}
    </section>
  );
}
