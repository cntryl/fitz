import { ChartPanel, ChartShell } from "@askrjs/charts/components";
import ChartMeter from "@/components/shared/chart-meter";
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
    { label: "Ready", value: stats.messagesReady },
    { label: "Inflight", value: stats.inflightActive },
    { label: "Pending", value: stats.messagesPending },
    { label: "Delayed", value: stats.messagesDelayed },
    { label: "Dead letters", value: stats.messagesDeadLettered },
  ];

  return (
    <ChartShell
      className="domain-chart-shell"
      title="Message state"
      description="Live queue message distribution."
    >
      <ChartPanel title="Queue state" description="Current message mix across queue states.">
        <div class="chart-meter-grid">
          {entries.map((entry) => (
            <div key={entry.label}>
              <ChartMeter
                label={entry.label}
                value={entry.value}
                max={max}
                valueFormatter={formatNumber}
              />
            </div>
          ))}
        </div>
      </ChartPanel>
    </ChartShell>
  );
}
