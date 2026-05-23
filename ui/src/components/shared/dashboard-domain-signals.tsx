import ChartMeter from "@/components/shared/chart-meter";
import { ChartPanel, ChartShell } from "@/components/shared/chart-frame";
import type { SystemOverview } from "@/features/system/system-models";
import { formatNumber } from "@/shared/format";

function formatRate(value: number) {
  return value.toFixed(2);
}

function currentVolumeData(overview: SystemOverview) {
  return [
    { description: "Ready messages", label: "Queue", value: overview.domains.queue.messagesReady },
    { description: "Tracked keys", label: "KV", value: overview.domains.kv.keysTotal },
    { description: "Active leases", label: "Lease", value: overview.domains.lease.leasesActive },
    {
      description: "Active subscriptions",
      label: "Notice",
      value: overview.domains.notice.subscriptionsActive,
    },
    { description: "Pending requests", label: "RPC", value: overview.domains.rpc.requestsPending },
    {
      description: "Pending claims",
      label: "Schedule",
      value: overview.domains.schedule.pendingFireClaims,
    },
    { description: "Total events", label: "Stream", value: overview.domains.stream.eventsTotal },
  ];
}

function activityRateData(overview: SystemOverview) {
  return [
    { label: "Queue", value: overview.domains.queue.operationsPerSecond },
    { label: "KV", value: overview.domains.kv.operationsPerSecond },
    { label: "Lease", value: overview.domains.lease.operationsPerSecond },
    { label: "Notice", value: overview.domains.notice.publishesPerSecond },
    { label: "RPC", value: overview.domains.rpc.operationsPerSecond },
    { label: "Schedule", value: overview.domains.schedule.executionsPerMinute / 60 },
    { label: "Stream", value: overview.domains.stream.operationsPerSecond },
  ];
}

function maxValue(values: { value: number }[]) {
  return Math.max(1, ...values.map((entry) => entry.value));
}

export default function DashboardDomainSignals({ overview }: { overview: SystemOverview }) {
  const volume = currentVolumeData(overview);
  const activity = activityRateData(overview);
  const volumeMax = maxValue(volume);
  const activityMax = maxValue(activity);

  return (
    <ChartShell
      className="domain-chart-shell"
      title="Domain signals"
      description="Live broker snapshot across domains."
    >
      <div class="chart-grid">
        <ChartPanel title="Current volume" description="Representative live counts by domain.">
          <div class="chart-meter-grid">
            {volume.map((entry) => (
              <div key={`volume-${entry.label}`}>
                <ChartMeter
                  label={entry.label}
                  value={entry.value}
                  max={volumeMax}
                  description={entry.description}
                  valueFormatter={formatNumber}
                />
              </div>
            ))}
          </div>
        </ChartPanel>

        <ChartPanel
          title="Activity rate"
          description="Current operations per second across domains."
        >
          <div class="chart-meter-grid">
            {activity.map((entry) => (
              <div key={`activity-${entry.label}`}>
                <ChartMeter
                  label={entry.label}
                  value={entry.value}
                  max={activityMax}
                  valueFormatter={formatRate}
                />
              </div>
            ))}
          </div>
        </ChartPanel>
      </div>
    </ChartShell>
  );
}
