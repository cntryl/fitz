import DomainInventoryPage from "@/components/shared/domain-inventory-page";
import type { DomainResourceMetricColumn } from "@/components/shared/domain-resource-inventory-table";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { createStreamOverviewQuery } from "@/features/stream/stream-query";
import type { StreamLagBucketsSummary } from "@/features/stream/stream-models";
import { formatNumber } from "@/shared/format";

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

function formatWatermarkLag(buckets: StreamLagBucketsSummary) {
  const lag = summarizeWatermarkLag(buckets);

  if (lag.total === 0) {
    return "--";
  }

  return `${formatNumber(lag.behind)} / ${formatNumber(lag.total)}`;
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
      detail: `${formatNumber(stats.subscriptionsActive)} live subscriptions are reading from ${formatNumber(
        stats.streamsActive,
      )} active stream(s). ${lag.percentageBehind}% of families are behind the latest watermark, including ${formatNumber(
        stats.watermarkLagBuckets.over100,
      )} family(s) at 100+ behind.`,
      label: "Attention",
      tone: "danger",
    };
  }

  if (lag.behind > 0) {
    return {
      detail: `${formatNumber(stats.subscriptionsActive)} live subscriptions are tracking ${formatNumber(
        stats.streamsActive,
      )} active stream(s). ${lag.percentageBehind}% of families are behind the latest watermark, and replay catch-up is in progress.`,
      label: "Pressure",
      tone: "warning",
    };
  }

  return {
    detail: `${formatNumber(stats.subscriptionsActive)} live subscriptions are fully caught up across ${formatNumber(
      stats.streamsActive,
    )} active stream(s). ${formatNumber(stats.eventsTotal)} committed events are durable in replay history.`,
    label: "Live",
    tone: "success",
  };
}

function resourceCount(data: ReturnType<typeof createResourceInventoryQuery>["data"]) {
  return (
    data?.realms.reduce(
      (sum, realm) =>
        sum + realm.areas.reduce((areaSum, area) => areaSum + area.resources.length, 0),
      0,
    ) ?? 0
  );
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
  const streamCount = resourceCount(inventory.data);
  const stats = overview.data?.stats;
  const streamMetricColumns: readonly DomainResourceMetricColumn[] = [
    {
      id: "events",
      header: "Events",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.eventsTotal) : "--"),
    },
    {
      id: "streams",
      header: "Streams",
      width: "10%",
      cell: () => (stats ? formatNumber(stats.streamsActive) : "--"),
    },
    {
      id: "subscriptions",
      header: "Subscriptions",
      width: "12%",
      cell: () => (stats ? formatNumber(stats.subscriptionsActive) : "--"),
    },
    {
      id: "watermark-lag",
      header: "Watermark lag",
      width: "13%",
      cell: () => (stats ? formatWatermarkLag(stats.watermarkLagBuckets) : "--"),
    },
    {
      id: "ops",
      header: "Ops / sec",
      width: "10%",
      cell: () => (stats ? stats.operationsPerSecond.toFixed(2) : "--"),
    },
  ];

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
      emptyDescription="No stream resources are currently visible."
      tableTitle="Resource inventory"
      metricColumns={streamMetricColumns}
      status={{
        detail: overview.data
          ? `${formatNumber(streamCount)} stream resource${streamCount === 1 ? "" : "s"} visible. ${
              health.detail
            }`
          : overview.error
            ? "Stream health is unavailable. Resource inventory can still be inspected when loaded."
            : "Loading stream health.",
        label: overview.refreshing
          ? "Refreshing"
          : overview.error
            ? "Health unavailable"
            : overview.stale
              ? "Stale"
              : health.label,
        tone: overview.refreshing
          ? "info"
          : overview.error
            ? "warning"
            : overview.stale
              ? "warning"
              : health.tone,
      }}
    />
  );
}
