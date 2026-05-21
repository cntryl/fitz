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
import SidebarLayout from "@/components/shared/sidebar-layout";
import { AlertTriangleIcon } from "@askrjs/lucide";
import { EmptyState, Spinner } from "@askrjs/themes/feedback";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { createRpcOverviewQuery } from "@/features/rpc/rpc-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatUnknownError } from "@/shared/errors/format";

function summarizeRpcPressure(workersRegistered: number, requestsPending: number) {
  if (workersRegistered === 0 && requestsPending > 0) {
    return {
      label: "Worker starvation",
      detail: `${requestsPending} pending requests have no registered workers to handle them.`,
    };
  }

  if (requestsPending > workersRegistered) {
    return {
      label: "Backpressure",
      detail: `${requestsPending} pending requests are ahead of ${workersRegistered} workers.`,
    };
  }

  return {
    label: "Stable",
    detail: "RPC pressure is not currently growing faster than worker capacity.",
  };
}

export default function RpcPage() {
  const overview = createRpcOverviewQuery();
  const inventory = createResourceInventoryQuery("rpc");
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
            {(() => {
              const pressure = summarizeRpcPressure(
                data.stats.workersRegistered,
                data.stats.requestsPending,
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

            <DomainResourceBrowser
              domain="rpc"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
