import { For } from "@askrjs/askr/control";
import { currentRoute, Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainResourceBrowser from "@/components/shared/domain-resource-browser";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import DomainWorkflowPanel from "@/components/shared/domain-workflow-panel";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Stack,
} from "@askrjs/themes/components";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
  QueryEmptyState,
} from "@/components/shared/query-state";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  createStreamAreaQuery,
  createStreamOverviewQuery,
  createStreamRealmQuery,
} from "@/features/stream/stream-query";
import { createResourceInventoryQuery } from "@/features/resource/resource-query";
import { formatNumber } from "@/shared/format";
import { domainResourceHref, domainScopeHref } from "@/shared/navigation/domains";
import type { StreamLagBucketsSummary } from "@/features/stream/stream-models";

function decodeParam(value: string | undefined) {
  if (!value) return undefined;

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

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

function StreamOverviewPage() {
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
  const snapshot = createDomainSidebar({
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
    <DomainPageFrame>
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

        {snapshot}

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

            <DomainRealmTable
              domain="stream"
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

            <DomainWorkflowPanel
              archetype="Stream History Explorer"
              workflows={["Explore", "Trace", "Replay"]}
              questions={["What happened?", "Which readers are behind?", "Can I replay it?"]}
              diagnostics={["Watermarks", "Replay lag", "Storage internals"]}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

function StreamWatermarkTable(props: { rows: Array<{ family: number; watermark: number }> }) {
  if (props.rows.length === 0) {
    return <QueryEmptyState description="No stream family watermarks are currently visible." />;
  }

  return (
    <Table>
      <TableHead>
        <TableRow>
          <TableHeaderCell>Route Family</TableHeaderCell>
          <TableHeaderCell>Watermark</TableHeaderCell>
        </TableRow>
      </TableHead>
      <TableBody>
        <For each={props.rows} by={(row) => row.family.toString()}>
          {(row) => (
            <TableRow>
              <TableCell>{formatNumber(row.family)}</TableCell>
              <TableCell>{formatNumber(row.watermark)}</TableCell>
            </TableRow>
          )}
        </For>
      </TableBody>
    </Table>
  );
}

function StreamRealmPage(props: { realm: string }) {
  const query = createStreamRealmQuery(props.realm);
  const data = query.data;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Stream realm"
          title={props.realm}
          description={`Area and watermark rollup for ${props.realm}.`}
          primaryAction={{ label: "Refresh realm", onPress: () => query.refresh() }}
          status={{
            detail: data
              ? `${data.areaCount} area(s), ${data.resourceCount} resource(s).`
              : "Loading stream realm.",
            label: query.refreshing ? "Refreshing" : query.stale ? "Stale" : "Live",
            tone: query.refreshing ? "info" : query.stale ? "warning" : "success",
          }}
        />
        {!data && query.loading ? (
          <QueryLoadingState description="Loading stream realm..." />
        ) : null}
        {!data && query.error ? (
          <QueryErrorState
            title="Unable to load stream realm"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        ) : null}
        {data ? (
          <Stack gap="3">
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Stream areas</CardTitle>
                <CardDescription>{data.areas.length} area(s)</CardDescription>
              </CardHeader>
              <CardContent>
                {data.areas.length === 0 ? (
                  <QueryEmptyState description="No visible stream areas at the current level." />
                ) : (
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Area</TableHeaderCell>
                        <TableHeaderCell>Resources</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      <For each={data.areas} by={(area) => area.area}>
                        {(area) => (
                          <TableRow>
                            <TableCell>
                              <Link
                                href={domainScopeHref("stream", {
                                  area: area.area,
                                  realm: data.realm,
                                })}
                              >
                                {area.area}
                              </Link>
                            </TableCell>
                            <TableCell>{formatNumber(area.resources.length)}</TableCell>
                          </TableRow>
                        )}
                      </For>
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Family watermarks</CardTitle>
                <CardDescription>Durable committed offsets by Route Family.</CardDescription>
              </CardHeader>
              <CardContent>
                <StreamWatermarkTable rows={data.familyWatermarks} />
              </CardContent>
            </Card>
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

function StreamAreaPage(props: { realm: string; area: string }) {
  const query = createStreamAreaQuery(props.realm, props.area);
  const data = query.data;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Stream area"
          title={props.area}
          description={`Resource and watermark rollup for ${props.realm}/${props.area}.`}
          primaryAction={{ label: "Refresh area", onPress: () => query.refresh() }}
          status={{
            detail: data ? `${data.resourceCount} resource(s).` : "Loading stream area.",
            label: query.refreshing ? "Refreshing" : query.stale ? "Stale" : "Live",
            tone: query.refreshing ? "info" : query.stale ? "warning" : "success",
          }}
        />
        {!data && query.loading ? <QueryLoadingState description="Loading stream area..." /> : null}
        {!data && query.error ? (
          <QueryErrorState
            title="Unable to load stream area"
            error={query.error}
            onRetry={() => query.refresh()}
          />
        ) : null}
        {data ? (
          <Stack gap="3">
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Stream resources</CardTitle>
                <CardDescription>{data.resources.length} resource(s)</CardDescription>
              </CardHeader>
              <CardContent>
                {data.resources.length === 0 ? (
                  <QueryEmptyState description="No visible stream resources at the current level." />
                ) : (
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Resource</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      <For each={data.resources} by={(resource) => resource}>
                        {(resource) => (
                          <TableRow>
                            <TableCell>
                              <Link
                                href={domainResourceHref("stream", {
                                  area: props.area,
                                  realm: props.realm,
                                  resource,
                                })}
                              >
                                {resource}
                              </Link>
                            </TableCell>
                          </TableRow>
                        )}
                      </For>
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Family watermarks</CardTitle>
                <CardDescription>Durable committed offsets by Route Family.</CardDescription>
              </CardHeader>
              <CardContent>
                <StreamWatermarkTable rows={data.familyWatermarks} />
              </CardContent>
            </Card>
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}

export default function StreamPage() {
  const route = currentRoute();
  const realm = decodeParam(route.params.realm);
  const area = decodeParam(route.params.area);

  if (realm && area) return <StreamAreaPage area={area} realm={realm} />;
  if (realm) return <StreamRealmPage realm={realm} />;

  return <StreamOverviewPage />;
}
