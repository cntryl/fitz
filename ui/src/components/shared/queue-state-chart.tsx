import { BarChart, ChartPanel, ChartShell } from "@askrjs/charts/components";
import type { QueueOverview } from "@/features/queue/queue-models";
import { formatNumber } from "@/shared/format";

export default function QueueStateChart({ stats }: { stats: QueueOverview["stats"] }) {
  const total =
    stats.messagesReady +
    stats.inflightActive +
    stats.messagesPending +
    stats.messagesDelayed +
    stats.messagesDeadLettered;
  const max = Math.max(1, total);
  const entries = [
    { label: "Ready", unitLabel: "messages", value: stats.messagesReady },
    { label: "Inflight", unitLabel: "messages", value: stats.inflightActive },
    { label: "Pending", unitLabel: "messages", value: stats.messagesPending },
    { label: "Delayed", unitLabel: "messages", value: stats.messagesDelayed },
    { label: "Dead letters", unitLabel: "messages", value: stats.messagesDeadLettered },
  ];

  return (
    <ChartShell
      className="domain-chart-shell"
      title="Message state"
      description="Current queue message distribution across durable and inflight states."
    >
      <ChartPanel
        title="Queue state"
        description="Read this as the current mix, not a history of how the queue got here."
      >
        <BarChart
          label="Live queue snapshot"
          max={max}
          summary={`${formatNumber(total)} current message signal(s).`}
          valueFormatter={formatNumber}
          data={entries.map((entry) => ({
            description: entry.unitLabel,
            label: entry.label,
            value: entry.value,
          }))}
        />
      </ChartPanel>
    </ChartShell>
  );
}
