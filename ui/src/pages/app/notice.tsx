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

export default function NoticePage() {
  const overview = createNoticeOverviewQuery();
  const inventory = createResourceInventoryQuery("notice");
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "Notice snapshot",
    description: "Live fanout activity and subscription coverage.",
    stats: (current) => [
      {
        label: "Publishes / sec",
        value: current.stats.publishesPerSecond.toFixed(2),
      },
      { label: "Subscriptions", value: current.stats.subscriptionsActive },
    ],
  });

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Live awareness"
          title="Notice overview"
          description="Live fanout pressure, subscription coverage, and realm inventory."
          primaryAction={{
            label: "Refresh notice",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: "Notice is live fanout only; delivery ends when the subscriber disconnects.",
            label: overview.refreshing ? "Refreshing" : overview.stale ? "Stale" : "Live",
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : "success",
          }}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading notice overview..." />
        ) : null}

        {!data && overview.error ? (
          <QueryErrorState error={overview.error} onRetry={() => overview.refresh()} />
        ) : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing notice overview..." />
            ) : null}

            <DomainMetricTable
              title="Notice metrics"
              description="Live fanout pressure and active subscription coverage."
              metrics={[
                {
                  label: "Publishes / sec",
                  value: data.stats.publishesPerSecond.toFixed(2),
                },
                { label: "Subscriptions", value: data.stats.subscriptionsActive },
              ]}
            />

            <DomainBarChart
              title="Notice signal"
              description="Current publish rate and live subscription footprint."
              label="Notice state snapshot"
              scope="Live notice snapshot"
              data={[
                {
                  label: "Publishes / sec",
                  unitLabel: "ops/sec",
                  value: data.stats.publishesPerSecond,
                },
                {
                  label: "Subscriptions",
                  unitLabel: "subscriptions",
                  value: data.stats.subscriptionsActive,
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
