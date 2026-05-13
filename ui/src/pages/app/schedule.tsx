import { SidebarLayout } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { AlertTriangleIcon } from "@askrjs/lucide";
import { EmptyState, Spinner } from "@askrjs/themes/components";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { createScheduleOverviewQuery } from "@/features/schedule/schedule-query";
import { formatUnknownError } from "@/shared/errors/format";

export default function SchedulePage() {
  const overview = createScheduleOverviewQuery();
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "Schedule snapshot",
    description: "Execution health and claim pressure across scheduled work.",
    stats: (current) => [
      { label: "Schedules", value: current.stats.schedulesActive },
      { label: "Subscriptions", value: current.stats.subscriptionsActive },
      { label: "Pending claims", value: current.stats.pendingFireClaims },
      {
        label: "Executions / min",
        value: current.stats.executionsPerMinute.toFixed(2),
        note: "Live broker snapshot",
      },
    ],
  });

  return (
    <SidebarLayout
      sidebar={sidebar}
      sidebarPosition="end"
      sidebarWidth="18rem"
      gap="1.5rem"
      collapseBelow="md"
    >
      <section class="domain-page">
        <DomainHeader
          domain="Schedule"
          title="Schedule overview"
          description="Scheduled execution health and live realm inventory."
          onRefresh={() => overview.refresh()}
        />

        {overview.loading ? (
          <EmptyState
            class="domain-state"
            icon={<Spinner label="Loading" />}
            description="Loading schedule overview..."
          />
        ) : null}

        {overview.error ? (
          <EmptyState
            class="domain-state"
            icon={<AlertTriangleIcon size={18} />}
            description={formatUnknownError(overview.error)}
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
    </SidebarLayout>
  );
}
