import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import DomainState from "@/components/shared/domain-state";
import { createNoticeOverviewQuery } from "@/features/notice/notice-query";

export default function NoticePage() {
  const overview = createNoticeOverviewQuery();
  const data = overview.data;

  return (
    <section class="domain-page">
      <DomainHeader
        domain="Notice"
        title="Notice overview"
        description="Notice fanout metrics and live realm inventory."
        onRefresh={() => overview.refresh()}
      />

      {overview.loading ? (
        <DomainState kind="loading" message="Loading notice overview..." />
      ) : null}

      {overview.error ? (
        <DomainState
          kind="error"
          message="Notice overview could not be loaded."
          error={overview.error}
        />
      ) : null}

      {data && !overview.loading && !overview.error ? (
        <>
          <DomainMetricTable
            title="Notice metrics"
            metrics={[
              {
                label: "Publishes / sec",
                value: data.stats.publishesPerSecond.toFixed(2),
              },
              { label: "Subscriptions", value: data.stats.subscriptionsActive },
            ]}
          />

          <DomainRealmTable
            title="Notice realms"
            realms={data.realms}
            emptyMessage="No notice realms are currently visible."
          />
        </>
      ) : null}
    </section>
  );
}
