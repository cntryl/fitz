import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import SidebarLayout from "@/components/shared/sidebar-layout";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { createNoticeOverviewQuery } from "@/features/notice/notice-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

export default function NoticePage() {
  const overview = createNoticeOverviewQuery();
  const inventory = createResourceInventoryQuery("notice");
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "Notice snapshot",
    description: "Fanout health and active subscription coverage.",
    stats: (current) => [
      {
        label: "Publishes / sec",
        value: current.stats.publishesPerSecond.toFixed(2),
      },
      { label: "Subscriptions", value: current.stats.subscriptionsActive },
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
          domain="Notice"
          title="Notice overview"
          description="Notice fanout metrics and live realm inventory."
          onRefresh={() => overview.refresh()}
        />

        {overview.loading ? (
          <QueryLoadingState description="Loading notice overview..." />
        ) : null}

        {overview.error ? (
          <QueryErrorState error={overview.error} />
        ) : null}

        {data && !overview.loading && !overview.error ? (
          <div class="domain-stack">
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

            <DomainResourceBrowser
              domain="notice"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </div>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
