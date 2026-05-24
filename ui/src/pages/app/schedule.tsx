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
import { createScheduleOverviewQuery } from "@/features/schedule/schedule-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

export default function SchedulePage() {
  const overview = createScheduleOverviewQuery();
  const inventory = createResourceInventoryQuery("schedule");
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
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          domain="Schedule"
          title="Schedule overview"
          description="Scheduled execution health and live realm inventory."
          onRefresh={() => overview.refresh()}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading schedule overview..." />
        ) : null}

        {!data && overview.error ? <QueryErrorState error={overview.error} /> : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing schedule overview..." />
            ) : null}

            <DomainMetricTable
              title="Schedule metrics"
              metrics={[
                { label: "Schedules", value: data.stats.schedulesActive },
                { label: "Subscriptions", value: data.stats.subscriptionsActive },
                { label: "Pending claims", value: data.stats.pendingFireClaims },
                {
                  label: "Create persist fails",
                  value: data.stats.createPersistenceFailuresTotal,
                },
                {
                  label: "Upsert persist fails",
                  value: data.stats.upsertPersistenceFailuresTotal,
                },
                {
                  label: "Cancel persist fails",
                  value: data.stats.cancelPersistenceFailuresTotal,
                },
                {
                  label: "Executions / min",
                  value: data.stats.executionsPerMinute.toFixed(2),
                },
              ]}
            />

            <DomainBarChart
              title="Schedule signal"
              description="Scheduled volume, subscriptions, claims, and execution rate."
              label="Schedule state snapshot"
              data={[
                ["Schedules", data.stats.schedulesActive],
                ["Subscriptions", data.stats.subscriptionsActive],
                ["Pending claims", data.stats.pendingFireClaims],
                ["Exec / min", data.stats.executionsPerMinute],
              ]}
            />

            <DomainRealmTable
              title="Schedule realms"
              realms={data.realms}
              emptyMessage="No schedule realms are currently visible."
            />

            <DomainResourceBrowser
              domain="schedule"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
