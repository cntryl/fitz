import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import DomainState from "@/components/shared/domain-state";
import { createScheduleOverviewQuery } from "@/features/schedule/schedule-query";

export default function SchedulePage() {
  const overview = createScheduleOverviewQuery();
  const data = overview.data;

  return (
    <section class="domain-page">
      <DomainHeader
        domain="Schedule"
        title="Schedule overview"
        description="Scheduled execution health and live realm inventory."
        onRefresh={() => overview.refresh()}
      />

      {overview.loading ? (
        <DomainState kind="loading" message="Loading schedule overview..." />
      ) : null}

      {overview.error ? (
        <DomainState
          kind="error"
          message="Schedule overview could not be loaded."
          error={overview.error}
        />
      ) : null}

      {data && !overview.loading && !overview.error ? (
        <>
          <DomainMetricTable
            title="Schedule metrics"
            metrics={[
              { label: "Schedules", value: data.stats.schedulesActive },
              { label: "Subscriptions", value: data.stats.subscriptionsActive },
              { label: "Pending claims", value: data.stats.pendingFireClaims },
              {
                label: "Executions / min",
                value: data.stats.executionsPerMinute.toFixed(2),
              },
            ]}
          />

          <DomainRealmTable
            title="Schedule realms"
            realms={data.realms}
            emptyMessage="No schedule realms are currently visible."
          />
        </>
      ) : null}
    </section>
  );
}
