import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import SidebarLayout from "@/components/shared/sidebar-layout";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { createStreamOverviewQuery } from "@/features/stream/stream-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

export default function StreamPage() {
  const overview = createStreamOverviewQuery();
  const inventory = createResourceInventoryQuery("stream");
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "Stream snapshot",
    description: "Stream throughput and subscription coverage.",
    stats: (current) => [
      { label: "Streams", value: current.stats.streamsActive },
      { label: "Subscriptions", value: current.stats.subscriptionsActive },
      { label: "Events", value: current.stats.eventsTotal },
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
          domain="Stream"
          title="Stream overview"
          description="Stream throughput, active subscriptions, and live realm inventory."
          onRefresh={() => overview.refresh()}
        />

        {overview.loading ? (
          <QueryLoadingState description="Loading stream overview..." />
        ) : null}

        {overview.error ? (
          <QueryErrorState error={overview.error} />
        ) : null}

        {data && !overview.loading && !overview.error ? (
          <div class="domain-stack">
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

            <DomainResourceBrowser
              domain="stream"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </div>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
