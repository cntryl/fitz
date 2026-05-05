import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import DomainState from "@/components/shared/domain-state";
import DomainSidebar from "@/components/shared/domain-sidebar";
import PageShell from "@/components/shared/page-shell";
import { createQueueOverviewQuery } from "@/features/queue/queue-query";

export default function QueuePage() {
  const overview = createQueueOverviewQuery();
  const data = overview.data;
  const sidebar = data ? (
    <DomainSidebar
      title="Queue snapshot"
      description="Current queue health across messages and broker activity."
      stats={[
        { label: "Ready", value: data.stats.messagesReady },
        { label: "Inflight", value: data.stats.inflightActive },
        { label: "Pending", value: data.stats.messagesPending },
        { label: "Dead-lettered", value: data.stats.messagesDeadLettered },
        { label: "Delayed", value: data.stats.messagesDelayed },
        {
          label: "Ops / sec",
          value: data.stats.operationsPerSecond.toFixed(2),
          note: "Live broker snapshot",
        },
      ]}
    />
  ) : undefined;

  return (
    <PageShell sidebar={sidebar}>
      <section class="domain-page">
        <DomainHeader
          domain="Queue"
          title="Queue overview"
          description="Live queue statistics and realm inventory. Resource-level dead-letter drill-down comes next."
          onRefresh={() => overview.refresh()}
        />

        {overview.loading ? (
          <DomainState kind="loading" message="Loading queue overview..." />
        ) : null}

        {overview.error ? (
          <DomainState
            kind="error"
            message="Queue overview could not be loaded."
            error={overview.error}
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
    </PageShell>
  );
}
