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
import { createStreamOverviewQuery } from "@/features/stream/stream-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";

export default function StreamPage() {
  const overview = createStreamOverviewQuery();
  const inventory = createResourceInventoryQuery("stream");
  const data = overview.data;
  const sidebar = createDomainSidebar({
    data,
    title: "Stream snapshot",
    description: "Durable history volume and live subscription coverage.",
    stats: (current) => [
      { label: "Streams", value: current.stats.streamsActive },
      { label: "Subscriptions", value: current.stats.subscriptionsActive },
      { label: "Events", value: current.stats.eventsTotal },
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
          eyebrow="Committed history"
          title="Stream overview"
          description="Committed stream history, live subscriptions, and realm inventory."
          primaryAction={{
            label: "Refresh stream",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: "Stream is the durable history and replay surface.",
            label: overview.refreshing ? "Refreshing" : overview.stale ? "Stale" : "Live",
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : "success",
          }}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading stream overview..." />
        ) : null}

        {!data && overview.error ? (
          <QueryErrorState error={overview.error} onRetry={() => overview.refresh()} />
        ) : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing stream overview..." />
            ) : null}

            <DomainMetricTable
              title="Stream metrics"
              description="Committed history volume, subscriptions, and append activity."
              metrics={[
                { label: "Streams", value: data.stats.streamsActive },
                { label: "Subscriptions", value: data.stats.subscriptionsActive },
                { label: "Events", value: data.stats.eventsTotal },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
              ]}
            />

            <DomainBarChart
              title="Stream signal"
              description="Current stream footprint, active subscriptions, and event volume."
              label="Stream state snapshot"
              scope="Live stream snapshot"
              data={[
                { label: "Streams", unitLabel: "streams", value: data.stats.streamsActive },
                {
                  label: "Subscriptions",
                  unitLabel: "subscriptions",
                  value: data.stats.subscriptionsActive,
                },
                { label: "Events", unitLabel: "events", value: data.stats.eventsTotal },
                {
                  label: "Ops / sec",
                  unitLabel: "ops/sec",
                  value: data.stats.operationsPerSecond,
                },
              ]}
            />

            <DomainRealmTable
              title="Stream realms"
              realms={data.realms}
              emptyMessage="No stream realms are currently visible."
            />

            <DomainResourceBrowser
              domain="stream"
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
