import { Stack } from "@askrjs/themes/layouts";
import DomainBarChart from "@/components/shared/domain-bar-chart";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createLeaseOverviewQuery } from "@/features/lease/lease-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

export default function LeasePage() {
  const overview = createLeaseOverviewQuery();
  const inventory = createResourceInventoryQuery("lease");
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "Lease snapshot",
    description: "Current ownership coordination and waiter pressure.",
    stats: (current) => [
      { label: "Active leases", value: current.stats.leasesActive },
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
          title="Lease overview"
          description="Current lease load, waiter pressure, and realm coverage."
          onRefresh={() => overview.refresh()}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading lease overview..." />
        ) : null}

        {!data && overview.error ? <QueryErrorState error={overview.error} /> : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing lease overview..." />
            ) : null}

            <DomainMetricTable
              title="Lease metrics"
              metrics={[
                { label: "Active leases", value: data.stats.leasesActive },
                { label: "Waiters", value: data.stats.waiterDepth },
                {
                  label: "Oldest lease age",
                  value: `${data.stats.oldestLeaseAgeSeconds}s`,
                },
                { label: "Acquire timeouts", value: data.stats.acquireTimeoutsTotal },
                { label: "Forced releases", value: data.stats.forcedReleasesTotal },
                { label: "Token rejects", value: data.stats.invalidTokenRejectsTotal },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
              ]}
            />

            <DomainBarChart
              title="Lease signal"
              description="Current lease load, waiter pressure, and oldest lease age."
              label="Lease state snapshot"
              data={[
                ["Active leases", data.stats.leasesActive],
                ["Waiters", data.stats.waiterDepth],
                ["Oldest age", data.stats.oldestLeaseAgeSeconds],
              ]}
            />

            <DomainRealmTable
              title="Lease realms"
              realms={data.realms}
              emptyMessage="No lease realms are currently visible."
            />

            <DomainResourceBrowser
              domain="lease"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
