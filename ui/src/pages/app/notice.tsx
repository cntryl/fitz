import DomainHeader from "@/components/shared/domain-header";
import DomainBarChart from "@/components/shared/domain-bar-chart";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import { Stack } from "@askrjs/themes/layouts";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { createNoticeOverviewQuery } from "@/features/notice/notice-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

function summarizeNoticeHealth(stats: {
  deliveryDropsTotal: number;
  subscriptionsActive: number;
  wildcardLimitRejectsTotal: number;
  publishesPerSecond: number;
  routesActive: number;
}) {
  const riskCount = stats.deliveryDropsTotal + stats.wildcardLimitRejectsTotal;
  const hasRisk = riskCount > 0;
  const pressureSignals = [
    stats.deliveryDropsTotal > 0 ? `${stats.deliveryDropsTotal} delivery drop(s)` : null,
    stats.wildcardLimitRejectsTotal > 0
      ? `${stats.wildcardLimitRejectsTotal} wildcard reject(s)`
      : null,
  ].filter((signal): signal is string => signal !== null);

  if (hasRisk) {
    return {
      detail: `${stats.subscriptionsActive} active subscriptions and ${stats.publishesPerSecond.toFixed(2)} publishes/sec. ${pressureSignals.join(", ")} are above healthy fanout baseline.`,
      label: "Attention" as const,
      tone: "danger" as const,
    };
  }

  return {
    detail: `${stats.subscriptionsActive} active subscriptions across ${stats.routesActive} route(s). ${stats.publishesPerSecond.toFixed(2)} publishes/sec is moving through live fanout.`,
    label: "Live" as const,
    tone: "success" as const,
  };
}

function metricWithRisk(value: number, label: string) {
  return {
    label,
    value,
    ...(value > 0 ? { caption: "attention" } : undefined),
  };
}

export default function NoticePage() {
  const overview = createNoticeOverviewQuery();
  const inventory = createResourceInventoryQuery("notice");
  const data = overview.data;
  const health = summarizeNoticeHealth(
    data?.stats ?? {
      deliveryDropsTotal: 0,
      subscriptionsActive: 0,
      wildcardLimitRejectsTotal: 0,
      publishesPerSecond: 0,
      routesActive: 0,
    },
  );
  const sidebar = createDomainSidebar({
    data,
    title: "Notice fanout snapshot",
    description: "Live subscription scope and fanout pressure diagnostics.",
    stats: (current) => [
      { label: "Visible notice realms", value: current.realms.length },
      { label: "Active routes", value: current.stats.routesActive },
      {
        label: "Publish rate",
        value: current.stats.publishesPerSecond.toFixed(2),
        note: "ops/sec",
      },
      { label: "Active subscriptions", value: current.stats.subscriptionsActive },
      {
        label: "Risk indicators",
        value: current.stats.deliveryDropsTotal + current.stats.wildcardLimitRejectsTotal,
      },
      {
        label: "Max route subscribers",
        value: current.stats.maxRouteSubscribers,
      },
    ],
  });

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Live awareness"
          title="Notice overview"
          description="Live fanout health, active subscription scope, and realm coverage."
          primaryAction={{
            label: "Refresh notice",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: `${health.detail} Notice is live fanout only; subscriptions expire on disconnect or restart.`,
            label: overview.refreshing ? "Refreshing" : overview.stale ? "Stale" : health.label,
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : health.tone,
          }}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading notice overview snapshot..." />
        ) : null}

        {!data && overview.error ? (
          <QueryErrorState
            title="Notice overview loading failure"
            error={overview.error}
            onRetry={() => overview.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing notice overview..." />
            ) : null}

            <DomainMetricTable
              title="Notice metrics"
              description="Live fanout health, publish, and subscription risk signals."
              metrics={[
                { label: "Active subscriptions", value: data.stats.subscriptionsActive },
                {
                  label: "Publish rate",
                  value: data.stats.publishesPerSecond.toFixed(2),
                },
                metricWithRisk(data.stats.deliveryDropsTotal, "Delivery drops"),
                metricWithRisk(data.stats.wildcardLimitRejectsTotal, "Wildcard limit rejects"),
              ]}
            />

            <DomainBarChart
              title="Notice signal"
              description="Current publish rate and fanout footprint."
              label="Notice state snapshot"
              scope="Live notice snapshot"
              data={[
                {
                  label: "Publish rate",
                  unitLabel: "ops/sec",
                  value: data.stats.publishesPerSecond,
                },
                {
                  label: "Active subscriptions",
                  unitLabel: "subscriptions",
                  value: data.stats.subscriptionsActive,
                },
                {
                  label: "Delivery drops",
                  unitLabel: "drops",
                  value: data.stats.deliveryDropsTotal,
                },
                {
                  label: "Wildcard rejects",
                  unitLabel: "rejects",
                  value: data.stats.wildcardLimitRejectsTotal,
                },
              ]}
            />

            <DomainRealmTable
              title="Notice realms"
              realms={data.realms}
              emptyMessage="No notice realms are currently visible."
            />

            <DomainResourceBrowser
              domain="notice"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
