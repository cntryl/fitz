import { Stack } from "@askrjs/themes/layouts";
import QueueStateChart from "@/components/shared/queue-state-chart";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import { createQueueOverviewQuery } from "@/features/queue/queue-query";
import type { QueueStatsSummary } from "@/features/queue/queue-models";
import { formatNumber } from "@/shared/format";

type QueuePostureTone = "info" | "success" | "warning" | "danger";

interface QueuePostureSummary {
  detail: string;
  label: string;
  nextStep: string;
  tone: QueuePostureTone;
}

function describeQueueOverview(stats: QueueStatsSummary): QueuePostureSummary {
  const ready = stats.messagesReady;
  const pending = stats.messagesPending;
  const delayed = stats.messagesDelayed;
  const inflight = stats.inflightActive;
  const deadLetters = stats.messagesDeadLettered;
  const backlog = ready + pending + delayed;

  const visibleCounts = [
    ready > 0 ? `${formatNumber(ready)} ready` : null,
    pending > 0 ? `${formatNumber(pending)} pending` : null,
    delayed > 0 ? `${formatNumber(delayed)} delayed` : null,
    inflight > 0 ? `${formatNumber(inflight)} inflight` : null,
  ].filter((value): value is string => value !== null);

  const stateSentence = visibleCounts.length
    ? `Visible work: ${visibleCounts.join(", ")}.`
    : "No ready, pending, delayed, or inflight work is visible.";

  const throughputSentence = `${formatNumber(stats.operationsPerSecond)} ops/sec are moving through the queue.`;

  if (deadLetters > 0) {
    return {
      detail: `${stateSentence} ${formatNumber(deadLetters)} dead-lettered messages need attention. ${throughputSentence}`,
      label: "Attention",
      nextStep: "Open the affected queue resource and inspect dead-letter handling first.",
      tone: "danger",
    };
  }

  if (backlog > inflight * 2 && backlog > 0) {
    return {
      detail: `${stateSentence} The backlog is outpacing inflight work. ${throughputSentence}`,
      label: "Pressure",
      nextStep: "Open the busiest queue resources and watch whether ready and pending counts drain.",
      tone: "warning",
    };
  }

  if (backlog > 0 || inflight > 0 || delayed > 0) {
    return {
      detail: `${stateSentence} Work is moving without dead-letter pressure. ${throughputSentence}`,
      label: "Healthy",
      nextStep: "Use the realm table if a narrower follow-up needs a closer look.",
      tone: "success",
    };
  }

  return {
    detail: `No backlog or inflight work is visible. ${throughputSentence}`,
    label: "Quiet",
    nextStep: "Use the realm table to confirm coverage when you want a narrower check.",
    tone: "success",
  };
}

export default function QueuePage() {
  const overview = createQueueOverviewQuery();
  const data = overview.data;
  const posture = data ? describeQueueOverview(data.stats) : null;

  const sidebar = createDomainSidebar({
    data,
    title: "Scope summary",
    description: "Backlog pressure and live queue activity.",
    stats: (current) => [
      { label: "Ready", value: current.stats.messagesReady },
      { label: "Inflight", value: current.stats.inflightActive },
      { label: "Pending", value: current.stats.messagesPending },
      { label: "Dead-lettered", value: current.stats.messagesDeadLettered },
      { label: "Delayed", value: current.stats.messagesDelayed },
      {
        label: "Ops / sec",
        value: current.stats.operationsPerSecond.toFixed(2),
        note: "Latest sample",
      },
    ],
  });

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Durable work"
          title="Queue overview"
          description="Current queue pressure, realm coverage, and the next place to inspect."
          primaryAction={{
            label: "Refresh queue",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: posture?.detail ?? "Durable backlog pressure and current queue activity.",
            label: overview.refreshing
              ? "Refreshing"
              : overview.stale
                ? "Stale"
                : posture?.label ?? "Live",
            tone: overview.refreshing
              ? "info"
              : overview.stale
                ? "warning"
                : posture?.tone ?? "success",
          }}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading queue overview..." />
        ) : null}

        {!data && overview.error ? (
          <QueryErrorState error={overview.error} onRetry={() => overview.refresh()} />
        ) : null}

        {data ? (
          <Stack gap="3">
            <DomainMetricTable
              title="Queue metrics"
              description="Current ready, inflight, pending, delayed, and dead-letter pressure."
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
