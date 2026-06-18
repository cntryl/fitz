import { Stack } from "@askrjs/themes/layouts";
import DomainBarChart from "@/components/shared/domain-bar-chart";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { formatDurationSeconds } from "@/shared/format";
import { createLeaseOverviewQuery } from "@/features/lease/lease-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

function summarizeRiskSignals(stats: {
  acquireTimeoutsTotal: number;
  forcedReleasesTotal: number;
  invalidTokenRejectsTotal: number;
  waiterDepth: number;
  oldestLeaseAgeSeconds: number;
  leasesActive: number;
}): {
  detail: string;
  label: "Live" | "Pressure" | "Attention";
  tone: "success" | "warning" | "danger";
} {
  const riskCount =
    stats.acquireTimeoutsTotal +
    stats.forcedReleasesTotal +
    stats.invalidTokenRejectsTotal +
    stats.waiterDepth;

  const hasRiskSignals =
    stats.acquireTimeoutsTotal > 0 ||
    stats.forcedReleasesTotal > 0 ||
    stats.invalidTokenRejectsTotal > 0;

  const label: "Live" | "Pressure" | "Attention" =
    riskCount > 6 ? "Attention" : hasRiskSignals || stats.waiterDepth > 0 ? "Pressure" : "Live";

  const tone: "success" | "warning" | "danger" =
    riskCount > 6 ? "danger" : hasRiskSignals || stats.waiterDepth > 0 ? "warning" : "success";

  const detailBase =
    `${stats.leasesActive} active leases, ${stats.waiterDepth} waiters, ${formatDurationSeconds(
      stats.oldestLeaseAgeSeconds,
    )} oldest lease age.` as string;

  if (hasRiskSignals) {
    const riskBits = [
      stats.acquireTimeoutsTotal > 0 ? `${stats.acquireTimeoutsTotal} acquire timeout(s)` : null,
      stats.forcedReleasesTotal > 0 ? `${stats.forcedReleasesTotal} forced release(s)` : null,
      stats.invalidTokenRejectsTotal > 0
        ? `${stats.invalidTokenRejectsTotal} token reject(s)`
        : null,
    ]
      .filter(Boolean)
      .join(", ");

    return {
      detail: `${detailBase} Attention is warranted: ${riskBits}.`,
      label,
      tone,
    };
  }

  if (stats.waiterDepth > 0) {
    return {
      detail: `${detailBase} Waiters are visible, so coordination pressure is elevated.`,
      label,
      tone,
    };
  }

  return {
    detail: `${detailBase} Coordination is stable and ephemeral by design.`,
    label,
    tone,
  };
}

function metricWithRisk(value: number, label: string) {
  return {
    label,
    value,
    ...(value > 0 ? { caption: "attention" } : undefined),
  };
}

export default function LeasePage() {
  const overview = createLeaseOverviewQuery();
  const inventory = createResourceInventoryQuery("lease");
  const data = overview.data;
  const riskSignals = summarizeRiskSignals(
    data?.stats ?? {
      acquireTimeoutsTotal: 0,
      forcedReleasesTotal: 0,
      invalidTokenRejectsTotal: 0,
      leasesActive: 0,
      oldestLeaseAgeSeconds: 0,
      waiterDepth: 0,
    },
  );

  const sidebar = createDomainSidebar({
    data,
    title: "Lease coordination snapshot",
    description: "Ephemeral ownership health and pressure diagnostics.",
    stats: (current) => [
      { label: "Visible lease realms", value: current.realms.length },
      {
        label: "Ownership pressure score",
        value:
          current.stats.acquireTimeoutsTotal +
          current.stats.forcedReleasesTotal +
          current.stats.invalidTokenRejectsTotal +
          current.stats.waiterDepth,
        note: "Derived risk signal",
      },
      {
        label: "Risk indicators",
        value:
          current.stats.acquireTimeoutsTotal +
          current.stats.forcedReleasesTotal +
          current.stats.invalidTokenRejectsTotal,
      },
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
          eyebrow="Ownership coordination"
          title="Lease overview"
          description="Current ephemeral ownership coordination, waiter pressure, and realm coverage."
          primaryAction={{
            label: "Refresh lease",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: `${riskSignals.detail} Leases must be reacquired after disconnect or restart.`,
            label: overview.refreshing
              ? "Refreshing"
              : overview.stale
                ? "Stale"
                : riskSignals.label,
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : riskSignals.tone,
          }}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading lease overview snapshot..." />
        ) : null}

        {!data && overview.error ? (
          <QueryErrorState
            title="Lease overview loading failure"
            error={overview.error}
            onRetry={() => overview.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing lease overview..." />
            ) : null}

            <DomainMetricTable
              title="Lease metrics"
              description="Leases and waiter pressure for the current snapshot."
              metrics={[
                { label: "Active leases", value: data.stats.leasesActive },
                { label: "Waiters", value: data.stats.waiterDepth },
                {
                  label: "Oldest lease age",
                  value: formatDurationSeconds(data.stats.oldestLeaseAgeSeconds),
                },
                {
                  label: "Ownership pressure",
                  value:
                    data.stats.waiterDepth +
                    data.stats.acquireTimeoutsTotal +
                    data.stats.forcedReleasesTotal +
                    data.stats.invalidTokenRejectsTotal,
                },
                metricWithRisk(data.stats.acquireTimeoutsTotal, "Acquire timeouts"),
                metricWithRisk(data.stats.forcedReleasesTotal, "Forced releases"),
                metricWithRisk(data.stats.invalidTokenRejectsTotal, "Token rejects"),
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
              ]}
            />

            <DomainBarChart
              title="Lease signal"
              description="Current lease ownership and waiter pressure."
              label="Lease state snapshot"
              scope="Live lease snapshot"
              data={[
                {
                  label: "Active leases",
                  unitLabel: "leases",
                  value: data.stats.leasesActive,
                },
                { label: "Waiters", unitLabel: "waiters", value: data.stats.waiterDepth },
                {
                  label: "Ownership pressure",
                  unitLabel: "pressure",
                  value:
                    data.stats.waiterDepth +
                    data.stats.acquireTimeoutsTotal +
                    data.stats.forcedReleasesTotal +
                    data.stats.invalidTokenRejectsTotal,
                },
              ]}
            />

            <DomainRealmTable
              title="Lease realms"
              realms={data.realms}
              emptyMessage="No lease realms are currently visible."
            />

            <DomainResourceBrowser
              domain="lease"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
