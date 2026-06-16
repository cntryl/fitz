import ChartMeter from "@/components/shared/chart-meter";
import { ChartPanel, ChartShell } from "@/components/shared/chart-frame";
import type { SystemOverview } from "@/features/system/system-models";
import { formatNumber } from "@/shared/format";

function formatRate(value: number) {
  return value.toFixed(2);
}

function currentVolumeData(overview: SystemOverview) {
  return [
    {
      description: "Ready messages",
      label: "Queue",
      unitLabel: "messages",
      value: overview.domains.queue.messagesReady,
    },
    {
      description: "Tracked keys",
      label: "KV",
      unitLabel: "keys",
      value: overview.domains.kv.keysTotal,
    },
    {
      description: "Active leases",
      label: "Lease",
      unitLabel: "leases",
      value: overview.domains.lease.leasesActive,
    },
    {
      description: "Active subscriptions",
      label: "Notice",
      unitLabel: "subscriptions",
      value: overview.domains.notice.subscriptionsActive,
    },
    {
      description: "Pending requests",
      label: "RPC",
      unitLabel: "requests",
      value: overview.domains.rpc.requestsPending,
    },
    {
      description: "Pending claims",
      label: "Schedule",
      unitLabel: "claims",
      value: overview.domains.schedule.pendingFireClaims,
    },
    {
      description: "Total events",
      label: "Stream",
      unitLabel: "events",
      value: overview.domains.stream.eventsTotal,
    },
  ];
}

function activityRateData(overview: SystemOverview) {
  return [
    { label: "Queue", unitLabel: "ops/sec", value: overview.domains.queue.operationsPerSecond },
    { label: "KV", unitLabel: "ops/sec", value: overview.domains.kv.operationsPerSecond },
    { label: "Lease", unitLabel: "ops/sec", value: overview.domains.lease.operationsPerSecond },
    { label: "Notice", unitLabel: "ops/sec", value: overview.domains.notice.publishesPerSecond },
    { label: "RPC", unitLabel: "ops/sec", value: overview.domains.rpc.operationsPerSecond },
    {
      label: "Schedule",
      unitLabel: "ops/sec",
      value: overview.domains.schedule.executionsPerMinute / 60,
    },
    { label: "Stream", unitLabel: "ops/sec", value: overview.domains.stream.operationsPerSecond },
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
      description="Live broker snapshot across domains, with counts and normalized activity rates."
      scope="Current broker snapshot"
    >
      <div class="chart-grid">
        <ChartPanel
          title="Current volume"
          description="Representative live counts by domain. Use this to see which domain currently carries the most state."
        >
          <div class="chart-meter-grid">
            {volume.map((entry) => (
              <div key={`volume-${entry.label}`}>
                <ChartMeter
                  label={entry.label}
                  value={entry.value}
                  max={volumeMax}
                  description={entry.description}
                  unitLabel={entry.unitLabel}
                  valueFormatter={formatNumber}
                />
              </div>
            ))}
          </div>
        </ChartPanel>

        <ChartPanel
          title="Activity rate"
          description="Normalized operations per second across domains."
        >
          <div class="chart-meter-grid">
            {activity.map((entry) => (
              <div key={`activity-${entry.label}`}>
                <ChartMeter
                  label={entry.label}
                  value={entry.value}
                  max={activityMax}
                  unitLabel={entry.unitLabel}
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
