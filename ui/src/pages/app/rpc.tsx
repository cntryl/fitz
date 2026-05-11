import { SidebarLayout } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { AlertTriangleIcon } from "@askrjs/lucide";
import { EmptyState, Spinner } from "@askrjs/themes/components";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { createRpcOverviewQuery } from "@/features/rpc/rpc-query";
import { formatUnknownError } from "@/shared/errors/format";

export default function RpcPage() {
  const overview = createRpcOverviewQuery();
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "RPC snapshot",
    description: "Worker registrations and pending request pressure.",
    stats: (current) => [
      { label: "Workers", value: current.stats.workersRegistered },
      { label: "Requests pending", value: current.stats.requestsPending },
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
          domain="RPC"
          title="RPC overview"
          description="Pending RPC work, worker registrations, and live realm inventory."
          onRefresh={() => overview.refresh()}
        />

        {overview.loading ? (
          <EmptyState
            class="domain-state"
            icon={<Spinner label="Loading" />}
            description="Loading RPC overview..."
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
              title="RPC metrics"
              metrics={[
                { label: "Workers", value: data.stats.workersRegistered },
                { label: "Requests pending", value: data.stats.requestsPending },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
              ]}
            />

            <DomainRealmTable
              title="RPC realms"
              realms={data.realms}
              emptyMessage="No RPC realms are currently visible."
            />
          </>
        ) : null}
      </section>
    </SidebarLayout>
  );
}