import { SidebarLayout } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { AlertTriangleIcon } from "@askrjs/lucide";
import { EmptyState, Spinner } from "@askrjs/themes/components";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { createQueueOverviewQuery } from "@/features/queue/queue-query";
import { formatUnknownError } from "@/shared/errors/format";

export default function QueuePage() {
  const overview = createQueueOverviewQuery();
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "Queue snapshot",
    description: "Current queue health across messages and broker activity.",
    stats: (current) => [
      { label: "Ready", value: current.stats.messagesReady },
      { label: "Inflight", value: current.stats.inflightActive },
      { label: "Pending", value: current.stats.messagesPending },
      { label: "Dead-lettered", value: current.stats.messagesDeadLettered },
      { label: "Delayed", value: current.stats.messagesDelayed },
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
          domain="Queue"
          title="Queue overview"
          description="Live queue statistics and realm inventory. Resource-level dead-letter drill-down comes next."
          onRefresh={() => overview.refresh()}
        />

        {overview.loading ? (
          <EmptyState
            class="domain-state"
            icon={<Spinner label="Loading" />}
            description="Loading queue overview..."
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
              title="Queue metrics"
              metrics={[
                { label: "Inflight", value: data.stats.inflightActive },
                { label: "Ready", value: data.stats.messagesReady },
                { label: "Pending", value: data.stats.messagesPending },
                { label: "Dead-lettered", value: data.stats.messagesDeadLettered },
                { label: "Delayed", value: data.stats.messagesDelayed },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
              ]}
            />

            <DomainRealmTable
              title="Queue realms"
              realms={data.realms}
              emptyMessage="No queue realms are currently visible."
            />
          </>
        ) : null}
      </section>
    </SidebarLayout>
  );
}