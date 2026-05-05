import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import DomainState from "@/components/shared/domain-state";
import { createStreamOverviewQuery } from "@/features/stream/stream-query";

export default function StreamPage() {
  const overview = createStreamOverviewQuery();
  const data = overview.data;

  return (
    <section class="domain-page">
      <DomainHeader
        domain="Stream"
        title="Stream overview"
        description="Stream throughput, active subscriptions, and live realm inventory."
        onRefresh={() => overview.refresh()}
      />

      {overview.loading ? (
        <DomainState kind="loading" message="Loading stream overview..." />
      ) : null}

      {overview.error ? (
        <DomainState
          kind="error"
          message="Stream overview could not be loaded."
          error={overview.error}
        />
      ) : null}

      {data && !overview.loading && !overview.error ? (
        <>
          <DomainMetricTable
            title="Stream metrics"
            metrics={[
              { label: "Streams", value: data.stats.streamsActive },
              { label: "Subscriptions", value: data.stats.subscriptionsActive },
              { label: "Events", value: data.stats.eventsTotal },
              {
                label: "Ops / sec",
                value: data.stats.operationsPerSecond.toFixed(2),
              },
            ]}
          />

          <DomainRealmTable
            title="Stream realms"
            realms={data.realms}
            emptyMessage="No stream realms are currently visible."
          />
        </>
      ) : null}
    </section>
  );
}
