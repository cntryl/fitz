import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import SidebarLayout from "@/components/shared/sidebar-layout";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { createLeaseOverviewQuery } from "@/features/lease/lease-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

function summarizeLeasePressure(leasesActive: number, waiterDepth: number, oldestLeaseAgeSeconds: number) {
  if (waiterDepth > 0) {
    return {
      label: "Lease contention",
      detail: `${waiterDepth} waiters are competing for ${leasesActive} active leases.`,
    };
  }

  if (oldestLeaseAgeSeconds > 0) {
    return {
      label: "Stale ownership risk",
      detail: `The oldest active lease is ${oldestLeaseAgeSeconds}s old.`,
    };
  }

  return {
    label: "Stable",
    detail: "Lease ownership pressure is not currently escalating.",
  };
}

export default function LeasePage() {
  const overview = createLeaseOverviewQuery();
  const inventory = createResourceInventoryQuery("lease");
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "Lease snapshot",
    description: "Live lease health and realm coverage.",
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
    <SidebarLayout
      sidebar={sidebar}
      sidebarPosition="end"
      sidebarWidth="18rem"
      gap="1.5rem"
      collapseBelow="md"
    >
      <section class="domain-page">
        <DomainHeader
          domain="Lease"
          title="Lease overview"
          description="Lease realm coverage and live lease load."
          onRefresh={() => overview.refresh()}
        />

        {overview.loading ? (
          <QueryLoadingState description="Loading lease overview..." />
        ) : null}

        {overview.error ? (
          <QueryErrorState error={overview.error} />
        ) : null}

        {data && !overview.loading && !overview.error ? (
          <div class="domain-stack">
            {(() => {
              const pressure = summarizeLeasePressure(
                data.stats.leasesActive,
                data.stats.waiterDepth,
                data.stats.oldestLeaseAgeSeconds,
              );

              return (
                <Card class="dashboard-status-card" variant="raised">
                  <CardHeader>
                    <CardTitle>Current pressure</CardTitle>
                    <CardDescription>{pressure.label}</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <p>{pressure.detail}</p>
                  </CardContent>
                </Card>
              );
            })()}

            <DomainMetricTable
              title="Lease metrics"
              metrics={[
                { label: "Active leases", value: data.stats.leasesActive },
                { label: "Waiters", value: data.stats.waiterDepth },
                {
                  label: "Oldest lease age",
                  value: `${data.stats.oldestLeaseAgeSeconds}s`,
                },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
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
          </div>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
