import { Stack } from "@askrjs/themes/layouts";
import QueueStateChart from "@/components/shared/queue-state-chart";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createQueueOverviewQuery } from "@/features/queue/queue-query";

export default function QueuePage() {
  const overview = createQueueOverviewQuery();
  const data = overview.data;

  const sidebar = createDomainSidebar({
    data,
    title: "Queue snapshot",
    description: "Durable backlog pressure and current queue activity.",
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
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          title="Queue overview"
          description="Current queue pressure, realms, and resource drill-down."
          onRefresh={() => overview.refresh()}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading queue overview..." />
        ) : null}

        {!data && overview.error ? <QueryErrorState error={overview.error} /> : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing queue overview..." />
            ) : null}

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

            <QueueStateChart stats={data.stats} />

            <DomainRealmTable
              title="Queue realms"
              realms={data.realms}
              emptyMessage="No queue realms are currently visible."
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
