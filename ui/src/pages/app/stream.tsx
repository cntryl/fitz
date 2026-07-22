import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import { queryHeaderStatus } from "@/components/shared/query-header-status";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { createStreamOverviewQuery } from "@/features/stream/stream-query";
import type { StreamLagBucketsSummary } from "@/features/stream/stream-models";
import { formatCount, formatNumber } from "@/shared/format";

type StreamPostureTone = "success" | "warning" | "danger" | "info";

interface StreamPosture {
  detail: string;
  label: "Live" | "Pressure" | "Attention";
  tone: StreamPostureTone;
}

function summarizeWatermarkLag(buckets: StreamLagBucketsSummary) {
  const total = buckets.caughtUp + buckets.under10 + buckets.under100 + buckets.over100;
  const behind = buckets.under10 + buckets.under100 + buckets.over100;

  return {
    behind,
    percentageBehind: total === 0 ? 0 : Math.round((behind / total) * 100),
    total,
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
        "No active stream families are visible yet; stream replay health will appear when families are active.",
      label: "Live",
      tone: "info",
    };
  }

  if (stats.watermarkLagBuckets.over100 > 0) {
    return {
      detail: `${formatCount(stats.eventsTotal, "committed event")} across ${formatCount(
        stats.streamsActive,
        "active stream",
      )}; ${formatCount(stats.subscriptionsActive, "live subscription")}. ${formatNumber(
        lag.behind,
      )} of ${formatNumber(
        lag.total,
      )} observed watermark families (${lag.percentageBehind}%) are behind the latest watermark, including ${formatNumber(
        stats.watermarkLagBuckets.over100,
      )} ${stats.watermarkLagBuckets.over100 === 1 ? "family" : "families"} at 100+ behind.`,
      label: "Attention",
      tone: "danger",
    };
  }

  if (lag.behind > 0) {
    return {
      detail: `${formatCount(stats.eventsTotal, "committed event")} across ${formatCount(
        stats.streamsActive,
        "active stream",
      )}; ${formatCount(stats.subscriptionsActive, "live subscription")}. ${formatNumber(
        lag.behind,
      )} of ${formatNumber(
        lag.total,
      )} observed watermark families (${lag.percentageBehind}%) are behind the latest watermark, and replay catch-up is in progress.`,
      label: "Pressure",
      tone: "warning",
    };
  }

  return {
    detail: `${formatCount(stats.eventsTotal, "committed event")} across ${formatCount(
      stats.streamsActive,
      "active stream",
    )}; ${formatCount(
      stats.subscriptionsActive,
      "live subscription",
    )}. Watermark lag is caught up for durable replay.`,
    label: "Live",
    tone: "success",
  };
}

export default function StreamPage() {
  const overview = createStreamOverviewQuery();
  const inventory = createResourceInventoryQuery("stream");
  const health = summarizeStreamHealth(
    overview.data?.stats ?? {
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
  const stats = overview.data?.stats;
  return (
    <DomainInventoryPage
      domain="stream"
      eyebrow="Durable replay"
      title="Stream inventory"
      description="Durable stream resources for replay and active live subscriptions."
      refreshLabel="Refresh stream"
      inventory={inventory}
      refreshing={overview.refreshing || inventory.refreshing}
      refreshers={[() => overview.refresh(), () => inventory.refresh()]}
      loadingDescription="Loading stream inventory..."
      errorTitle="Unable to load stream inventory"
      refreshingDescription="Refreshing stream inventory..."
      emptyDescription="No stream resources are currently visible. Check the selected Route Family or broaden scope."
      tableTitle="Resource inventory"
      stats={[
        { label: "Committed events", value: stats ? formatNumber(stats.eventsTotal) : "--" },
        { label: "Streams", value: stats ? formatNumber(stats.streamsActive) : "--" },
        { label: "Subscriptions", value: stats ? formatNumber(stats.subscriptionsActive) : "--" },
      ]}
      status={queryHeaderStatus(
        overview,
        {
          loading: "Loading stream health.",
          ready: health.detail,
          unavailable:
            "Stream health is unavailable. Resource inventory can still be inspected when loaded.",
        },
        { label: health.label, tone: health.tone },
      )}
    />
  );
}
