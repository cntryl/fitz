import DomainHeader from "@/components/shared/domain-header";
import DomainBarChart from "@/components/shared/domain-bar-chart";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import DomainWorkflowPanel from "@/components/shared/domain-workflow-panel";
import { Stack } from "@askrjs/themes/layouts";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import StreamHistoryExplorer from "@/features/stream/stream-history-explorer";
import { createStreamOverviewQuery } from "@/features/stream/stream-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatNumber } from "@/shared/format";
import type { StreamLagBucketsSummary } from "@/features/stream/stream-models";

type StreamPostureTone = "success" | "warning" | "danger" | "info";

interface StreamPosture {
  detail: string;
  label: "Live" | "Pressure" | "Attention";
  tone: StreamPostureTone;
}

function summarizeWatermarkLag(buckets: StreamLagBucketsSummary) {
  const total = buckets.caughtUp + buckets.under10 + buckets.under100 + buckets.over100;
  const behind = buckets.under10 + buckets.under100 + buckets.over100;
  const bucketsText = [
    buckets.caughtUp > 0 ? `${buckets.caughtUp} caught up` : null,
    buckets.under10 > 0 ? `${buckets.under10} behind <10` : null,
    buckets.under100 > 0 ? `${buckets.under100} behind 10-99` : null,
    buckets.over100 > 0 ? `${buckets.over100} behind 100+` : null,
  ].filter((entry): entry is string => entry !== null);

  return {
    behind,
    detail: bucketsText.length > 0 ? bucketsText.join(", ") : "No watermark samples",
    percentageBehind: total === 0 ? 0 : Math.round((behind / total) * 100),
    total,
    valueText:
      total === 0 ? "No watermark samples" : `${formatNumber(behind)} / ${formatNumber(total)}`,
  };
}

function summarizeStreamHealth(stats: {
  eventsTotal: number;
  operationsPerSecond: number;
  streamsActive: number;
  subscriptionsActive: number;
  watermarkLagBuckets: StreamLagBucketsSummary;
}): StreamPosture {
  const lag = summarizeWatermarkLag(stats.watermarkLagBuckets);

  if (lag.total === 0) {
    return {
      detail:
        "No active stream families are visible yet; stream replay health will appear here when families are active.",
      label: "Live",
      tone: "info",
    };
  }

  if (stats.watermarkLagBuckets.over100 > 0) {
    return {
      detail: `${stats.subscriptionsActive} live subscriptions are reading from ${stats.streamsActive} active stream(s). ${lag.percentageBehind}% of families are behind the latest watermark, including ${stats.watermarkLagBuckets.over100} family(s) at 100+ behind.`,
      label: "Attention",
      tone: "danger",
    };
  }

  if (lag.behind > 0) {
    return {
      detail: `${stats.subscriptionsActive} live subscriptions are tracking ${stats.streamsActive} active stream(s). ${lag.percentageBehind}% of families are behind the latest watermark, and replay catch-up is in progress.`,
      label: "Pressure",
      tone: "warning",
    };
  }

  return {
    detail: `${stats.subscriptionsActive} live subscriptions are fully caught up across ${stats.streamsActive} active stream(s). ${formatNumber(stats.eventsTotal)} committed events are durable in replay history.`,
    label: "Live",
    tone: "success",
  };
}

export default function StreamPage() {
  const overview = createStreamOverviewQuery();
  const inventory = createResourceInventoryQuery("stream");
  const data = overview.data;
  const health = summarizeStreamHealth(
    data?.stats ?? {
      eventsTotal: 0,
      operationsPerSecond: 0,
      streamsActive: 0,
      subscriptionsActive: 0,
      watermarkLagBuckets: {
        caughtUp: 0,
        under10: 0,
        under100: 0,
        over100: 0,
      },
    },
  );
  const lagBuckets = data ? summarizeWatermarkLag(data.stats.watermarkLagBuckets) : null;
  const sidebar = createDomainSidebar({
    data,
    title: "Stream snapshot",
    description: "Durable history and replay posture with live reader coverage.",
    stats: (current) => [
      { label: "Visible stream realms", value: current.realms.length },
      { label: "Active streams", value: current.stats.streamsActive },
      { label: "Active subscriptions", value: current.stats.subscriptionsActive },
      {
        label: "Watermark lag",
        value: summarizeWatermarkLag(current.stats.watermarkLagBuckets).valueText,
        note: summarizeWatermarkLag(current.stats.watermarkLagBuckets).detail,
      },
      { label: "Events total", value: current.stats.eventsTotal },
      {
        label: "Ops / sec",
        value: current.stats.operationsPerSecond.toFixed(2),
        note: "Live broker sample",
      },
    ],
  });

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Durable replay"
          title="Stream overview"
          description="Durable stream history for replay and active live subscriptions for readers."
          primaryAction={{
            label: "Refresh stream",
            onPress: () => overview.refresh(),
          }}
          status={{
            detail: health.detail,
            label: overview.refreshing ? "Refreshing" : overview.stale ? "Stale" : health.label,
            tone: overview.refreshing ? "info" : overview.stale ? "warning" : health.tone,
          }}
        />

        {!data && overview.loading ? (
          <QueryLoadingState description="Loading stream overview..." />
        ) : null}

        {!data && overview.error ? (
          <QueryErrorState
            title="Unable to load Stream overview"
            error={overview.error}
            onRetry={() => overview.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            {overview.refreshing ? (
              <QueryRefreshingState description="Refreshing stream overview..." />
            ) : null}

            <DomainWorkflowPanel
              archetype="Stream History Explorer"
              workflows={["Explore", "Trace", "Replay"]}
              questions={["What happened?", "Who consumed it?", "Can I replay it?"]}
              diagnostics={["Watermarks", "Replay lag", "Storage internals"]}
            />

            <StreamHistoryExplorer
              error={inventory.error}
              inventory={inventory.data}
              loading={inventory.loading}
            />

            <DomainMetricTable
              title="Stream metrics"
              description="Durable history, live readers, and replay lag."
              metrics={[
                { label: "Events total", value: data.stats.eventsTotal },
                { label: "Active streams", value: data.stats.streamsActive },
                { label: "Active subscriptions", value: data.stats.subscriptionsActive },
                {
                  label: "Watermark lag",
                  value: lagBuckets?.valueText ?? "No watermark samples",
                  caption: lagBuckets?.detail ?? "No watermark samples",
                },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
              ]}
            />

            <DomainBarChart
              title="Stream signal"
              description="Replay coverage, stream footprint, and lag posture."
              label="Stream state snapshot"
              scope="Live stream snapshot"
              data={[
                { label: "Active streams", unitLabel: "streams", value: data.stats.streamsActive },
                {
                  label: "Active subscriptions",
                  unitLabel: "subscriptions",
                  value: data.stats.subscriptionsActive,
                },
                { label: "Events", unitLabel: "events", value: data.stats.eventsTotal },
                {
                  label: "Watermark lag families",
                  unitLabel: "families",
                  value: lagBuckets?.behind ?? 0,
                },
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
              error={inventory.error}
              inventory={inventory.data}
              loading={inventory.loading}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
