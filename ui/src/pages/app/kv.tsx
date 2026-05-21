import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import SidebarLayout from "@/components/shared/sidebar-layout";
import { AlertTriangleIcon } from "@askrjs/lucide";
import { EmptyState, Spinner } from "@askrjs/themes/feedback";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { createKvOverviewQuery } from "@/features/kv/kv-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatUnknownError } from "@/shared/errors/format";

export default function KvPage() {
  const overview = createKvOverviewQuery();
  const inventory = createResourceInventoryQuery("kv");
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "KV snapshot",
    description: "Current key-value broker state and realm inventory.",
    stats: (current) => [
      { label: "Keys", value: current.stats.keysTotal },
      { label: "Transactions", value: current.stats.transactionsActive },
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
          domain="KV"
          title="KV overview"
          description="Key-value broker statistics and live realm inventory."
          onRefresh={() => overview.refresh()}
        />

        {overview.loading ? (
          <EmptyState
            class="domain-state"
            icon={<Spinner label="Loading" />}
            description="Loading KV overview..."
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

            <DomainResourceBrowser
              domain="kv"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
