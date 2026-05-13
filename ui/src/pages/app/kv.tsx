import { SidebarLayout } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { AlertTriangleIcon } from "@askrjs/lucide";
import { EmptyState, Spinner } from "@askrjs/themes/components";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { createKvOverviewQuery } from "@/features/kv/kv-query";
import { formatUnknownError } from "@/shared/errors/format";

export default function KvPage() {
  const overview = createKvOverviewQuery();
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
          </>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
