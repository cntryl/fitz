import { For } from "@askrjs/askr/control";
import { state } from "@askrjs/askr";
import {
  Input,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeaderCell,
  TableRow,
} from "@askrjs/ui";
import { Button } from "@askrjs/themes/controls";
import { Stack } from "@askrjs/themes/layouts";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import type { PrometheusMetricFamily } from "@/features/metrics/metrics-models";
import { createMetricsOverviewQuery } from "@/features/metrics/metrics-query";
import { formatNumber } from "@/shared/format";

type MetricsHeaderTone = "default" | "info" | "success" | "warning" | "danger";

type MetricsSampleRow = {
  family: string;
  labels: string;
  type: string;
  value: number;
};

interface MetricsPostureSummary {
  detail: string;
  label: string;
  nextStep: string;
  tone: MetricsHeaderTone;
}

interface MetricsShortcut {
  label: string;
  query: string;
  test: (name: string) => boolean;
}

const metricsShortcuts: MetricsShortcut[] = [
  { label: "Queue", query: "queue", test: (name: string) => name.includes("queue") },
  { label: "RPC", query: "rpc", test: (name: string) => name.includes("rpc") },
  { label: "Stream", query: "stream", test: (name: string) => name.includes("stream") },
  { label: "Lease", query: "lease", test: (name: string) => name.includes("lease") },
  { label: "Notice", query: "notice", test: (name: string) => name.includes("notice") },
  { label: "KV", query: "kv", test: (name: string) => name.includes("kv") },
  {
    label: "Failures",
    query: "fail",
    test(name: string) {
      return /\b(fail|reject|drop|timeout|rollback|invalid|wrong|missing|error)\b/.test(name);
    },
  },
  {
    label: "Counters",
    query: "_total",
    test(name: string) {
      return name.endsWith("_total") || name.includes("total");
    },
  },
];

const emptySummaryCards = {
  broker: [],
  delivery: [],
  coordination: [],
  state: [],
  failures: [],
};

function buildFamilyIndex(families: PrometheusMetricFamily[]) {
  return new Map(families.map((family) => [family.name, family]));
}

function familyValue(index: Map<string, PrometheusMetricFamily>, name: string) {
  return index.get(name)?.samples.reduce((sum, sample) => sum + sample.value, 0) ?? 0;
}

function signalValue(
  index: Map<string, PrometheusMetricFamily>,
  names: string[],
) {
  return names.reduce((sum, name) => sum + familyValue(index, name), 0);
}

function signalText(label: string, value: number) {
  return `${label} ${formatNumber(value)}`;
}

function summarizeSnapshot(index: Map<string, PrometheusMetricFamily>): MetricsPostureSummary {
  const failureSignals = [
    { label: "router backpressure", value: signalValue(index, [
      "fitz_router_backpressure_total",
      "fitz_router_high_lane_backpressure_total",
    ]) },
    { label: "queue drops", value: signalValue(index, [
      "fitz_queue_notify_drops_total",
      "fitz_queue_redeliveries_total",
    ]) },
    { label: "RPC failures", value: signalValue(index, [
      "fitz_rpc_backpressure_rejects_total",
      "fitz_rpc_request_timeouts_total",
      "fitz_rpc_responses_dropped_closed_caller_total",
      "fitz_rpc_responses_missing_pending_total",
      "fitz_rpc_invalid_sequence_responses_total",
      "fitz_rpc_invalid_sequence_errors_forwarded_total",
      "fitz_rpc_invalid_sequence_errors_dropped_total",
      "fitz_rpc_wrong_worker_rejects_total",
    ]) },
    { label: "lease failures", value: signalValue(index, [
      "fitz_lease_acquire_timeouts_total",
      "fitz_lease_forced_releases_total",
      "fitz_lease_invalid_token_rejects_total",
    ]) },
    { label: "notice failures", value: signalValue(index, [
      "fitz_notice_delivery_drops_total",
      "fitz_notice_wildcard_limit_rejects_total",
    ]) },
    { label: "schedule failures", value: signalValue(index, [
      "fitz_schedule_notify_failures_total",
      "fitz_schedule_ack_failures_total",
      "fitz_schedule_create_persistence_failures_total",
      "fitz_schedule_upsert_persistence_failures_total",
      "fitz_schedule_cancel_persistence_failures_total",
      "fitz_schedule_pending_claim_cleanup_failure_total",
    ]) },
    { label: "stream drops", value: signalValue(index, [
      "fitz_stream_notify_drops_total",
      "fitz_stream_append_conflicts_total",
    ]) },
    { label: "KV failures", value: signalValue(index, [
      "fitz_kv_commits_failed_total",
      "fitz_kv_rollbacks_total",
      "fitz_kv_invalid_transaction_rejects_total",
    ]) },
  ];

  const pressureSignals = [
    { label: "queue backlog", value: signalValue(index, [
      "fitz_queue_ready_gauge",
      "fitz_queue_inflight_active",
      "fitz_queue_messages_pending",
      "fitz_queue_delayed_gauge",
    ]) },
    { label: "RPC pending", value: familyValue(index, "fitz_rpc_requests_pending") },
    { label: "lease waiters", value: familyValue(index, "fitz_lease_waiter_depth") },
    {
      label: "schedule claims",
      value: signalValue(index, [
        "fitz_schedule_pending_fire_claims",
        "fitz_schedule_pending_ack_retries",
      ]),
    },
    {
      label: "stream sessions",
      value: familyValue(index, "fitz_stream_append_sessions_active"),
    },
    { label: "KV transactions", value: familyValue(index, "fitz_kv_transactions_active") },
    {
      label: "notice subscriptions",
      value: familyValue(index, "fitz_notice_subscriptions_active"),
    },
  ];

  const activeFailures = failureSignals.filter((signal) => signal.value > 0);
  if (activeFailures.length > 0) {
    return {
      detail: `${activeFailures.length} failure groups are non-zero: ${activeFailures
        .slice(0, 3)
        .map((signal) => signalText(signal.label, signal.value))
        .join(", ")}.`,
      label: "Attention",
      nextStep:
        "Open the failure counters table first, then inspect the related queue, RPC, lease, notice, schedule, stream, or KV surface.",
      tone: "danger" as const,
    };
  }

  const activePressure = pressureSignals.filter((signal) => signal.value > 0);
  if (activePressure.length > 0) {
    return {
      detail: `${activePressure.length} pressure signals are active: ${activePressure
        .slice(0, 3)
        .map((signal) => signalText(signal.label, signal.value))
        .join(", ")}.`,
      label: "Pressure",
      nextStep:
        "Open the delivery pressure and coordination state cards to see where load is building.",
      tone: "warning" as const,
    };
  }

  return {
    detail: "No backlog, contention, or failure pressure detected.",
    label: "Quiet",
    nextStep: "Use the search box to inspect a specific metric family when you need a narrower read.",
    tone: "success",
  };
}

function familyCardMetrics(index: Map<string, PrometheusMetricFamily>) {
  return {
    broker: [
      { label: "Uptime", value: familyValue(index, "fitz_uptime_seconds"), caption: "seconds" },
      {
        label: "Connections",
        value: familyValue(index, "fitz_connections_total"),
        caption: "open",
      },
      {
        label: "Sessions",
        value: familyValue(index, "fitz_sessions_total"),
        caption: "active",
      },
      {
        label: "Messages received",
        value: familyValue(index, "fitz_messages_received_total"),
        caption: "lifetime total",
      },
      {
        label: "Messages sent",
        value: familyValue(index, "fitz_messages_sent_total"),
        caption: "lifetime total",
      },
      {
        label: "Router backpressure",
        value: familyValue(index, "fitz_router_backpressure_total"),
        caption: "drops",
      },
      {
        label: "High-lane backpressure",
        value: familyValue(index, "fitz_router_high_lane_backpressure_total"),
        caption: "drops",
      },
    ],
    delivery: [
      {
        label: "Queue ready",
        value: familyValue(index, "fitz_queue_ready_gauge"),
        caption: "messages",
      },
      {
        label: "Queue inflight",
        value: familyValue(index, "fitz_queue_inflight_active"),
        caption: "messages",
      },
      {
        label: "Queue pending",
        value: familyValue(index, "fitz_queue_messages_pending"),
        caption: "messages",
      },
      {
        label: "Queue delayed",
        value: familyValue(index, "fitz_queue_delayed_gauge"),
        caption: "messages",
      },
      {
        label: "Queue oldest message age",
        value: familyValue(index, "fitz_queue_oldest_message_age_seconds"),
        caption: "seconds",
      },
      {
        label: "Queue backlog age",
        value: familyValue(index, "fitz_queue_oldest_backlog_age_seconds"),
        caption: "seconds",
      },
      {
        label: "RPC workers",
        value: familyValue(index, "fitz_rpc_workers_registered"),
        caption: "registered",
      },
      {
        label: "RPC pending",
        value: familyValue(index, "fitz_rpc_requests_pending"),
        caption: "requests",
      },
      {
        label: "RPC oldest pending age",
        value: familyValue(index, "fitz_rpc_oldest_pending_request_age_seconds"),
        caption: "seconds",
      },
    ],
    coordination: [
      {
        label: "Lease active",
        value: familyValue(index, "fitz_lease_active"),
        caption: "claims",
      },
      {
        label: "Lease waiters",
        value: familyValue(index, "fitz_lease_waiter_depth"),
        caption: "waiters",
      },
      {
        label: "Lease oldest age",
        value: familyValue(index, "fitz_lease_oldest_lease_age_seconds"),
        caption: "seconds",
      },
      {
        label: "Schedule active",
        value: familyValue(index, "fitz_schedule_active"),
        caption: "jobs",
      },
      {
        label: "Schedule pending claims",
        value: familyValue(index, "fitz_schedule_pending_fire_claims"),
        caption: "claims",
      },
      {
        label: "Schedule ack retries",
        value: familyValue(index, "fitz_schedule_pending_ack_retries"),
        caption: "retries",
      },
      {
        label: "Stream append sessions",
        value: familyValue(index, "fitz_stream_append_sessions_active"),
        caption: "sessions",
      },
      {
        label: "Stream subscriptions",
        value: familyValue(index, "fitz_stream_subscriptions_active"),
        caption: "subscriptions",
      },
    ],
    state: [
      {
        label: "KV keys",
        value: familyValue(index, "fitz_kv_keys_total"),
        caption: "keys",
      },
      {
        label: "KV transactions",
        value: familyValue(index, "fitz_kv_transactions_active"),
        caption: "active",
      },
      {
        label: "Notice subscriptions",
        value: familyValue(index, "fitz_notice_subscriptions_active"),
        caption: "subscriptions",
      },
      {
        label: "Notice routes",
        value: familyValue(index, "fitz_notice_routes_active"),
        caption: "routes",
      },
      {
        label: "Notice peak subscribers",
        value: familyValue(index, "fitz_notice_max_route_subscribers"),
        caption: "peak",
      },
      {
        label: "Stream active",
        value: familyValue(index, "fitz_stream_active"),
        caption: "streams",
      },
      {
        label: "Stream events",
        value: familyValue(index, "fitz_stream_events_total"),
        caption: "committed",
      },
      {
        label: "Schedule executions / min",
        value: familyValue(index, "fitz_schedule_executions_per_minute").toFixed(2),
        caption: "per minute",
      },
    ],
    failures: [
      {
        label: "Queue redeliveries",
        value: familyValue(index, "fitz_queue_redeliveries_total"),
        caption: "events",
      },
      {
        label: "Queue notify drops",
        value: familyValue(index, "fitz_queue_notify_drops_total"),
        caption: "drops",
      },
      {
        label: "RPC backpressure rejects",
        value: familyValue(index, "fitz_rpc_backpressure_rejects_total"),
        caption: "drops",
      },
      {
        label: "RPC request timeouts",
        value: familyValue(index, "fitz_rpc_request_timeouts_total"),
        caption: "timeouts",
      },
      {
        label: "RPC missing pending",
        value: familyValue(index, "fitz_rpc_responses_missing_pending_total"),
        caption: "responses",
      },
      {
        label: "Lease acquire timeouts",
        value: familyValue(index, "fitz_lease_acquire_timeouts_total"),
        caption: "timeouts",
      },
      {
        label: "Lease forced releases",
        value: familyValue(index, "fitz_lease_forced_releases_total"),
        caption: "releases",
      },
      {
        label: "Lease invalid tokens",
        value: familyValue(index, "fitz_lease_invalid_token_rejects_total"),
        caption: "rejects",
      },
      {
        label: "Notice delivery drops",
        value: familyValue(index, "fitz_notice_delivery_drops_total"),
        caption: "drops",
      },
      {
        label: "Notice wildcard rejects",
        value: familyValue(index, "fitz_notice_wildcard_limit_rejects_total"),
        caption: "rejects",
      },
      {
        label: "Schedule notify failures",
        value: familyValue(index, "fitz_schedule_notify_failures_total"),
        caption: "failures",
      },
      {
        label: "Schedule ack failures",
        value: familyValue(index, "fitz_schedule_ack_failures_total"),
        caption: "failures",
      },
      {
        label: "Stream notify drops",
        value: familyValue(index, "fitz_stream_notify_drops_total"),
        caption: "drops",
      },
      {
        label: "KV commit failures",
        value: familyValue(index, "fitz_kv_commits_failed_total"),
        caption: "failures",
      },
      {
        label: "KV rollbacks",
        value: familyValue(index, "fitz_kv_rollbacks_total"),
        caption: "rollbacks",
      },
    ],
  };
}

function familyNameMatches(familyName: string, query: string) {
  return query.length === 0 || familyName.toLowerCase().includes(query);
}

function formatLabels(labels: Record<string, string>) {
  const entries = Object.entries(labels).sort(([left], [right]) => left.localeCompare(right));

  if (entries.length === 0) {
    return "n/a";
  }

  return entries.map(([key, value]) => `${key}="${value}"`).join(", ");
}

function buildRows(families: PrometheusMetricFamily[], filterValue: string): MetricsSampleRow[] {
  return families
    .filter((family) => familyNameMatches(family.name, filterValue))
    .flatMap((family) =>
      family.samples.map((sample) => ({
        family: sample.name ?? family.name,
        labels: formatLabels(sample.labels),
        type: family.type ?? "unknown",
        value: sample.value,
      })),
    );
}

function buildSummaryShortcuts(families: PrometheusMetricFamily[]) {
  return metricsShortcuts
    .map((shortcut) => ({
      label: shortcut.label,
      query: shortcut.query,
      count: families.filter((family) => shortcut.test(family.name.toLowerCase())).length,
      test: shortcut.test,
    }))
    .filter((shortcut) => shortcut.count > 0);
}

export default function MetricsPage() {
  const metrics = createMetricsOverviewQuery();
  const [filter, setFilter] = state("");
  const data = metrics.data;
  const filterValue = filter().trim().toLowerCase();
  const familyIndex = data ? buildFamilyIndex(data.families) : null;
  const families =
    data?.families.filter((family) => familyNameMatches(family.name.toLowerCase(), filterValue)) ?? [];
  const sampleRows = data ? buildRows(data.families, filterValue) : [];
  const sampleCount = data?.families.reduce((sum, family) => sum + family.samples.length, 0) ?? 0;
  const snapshotSummary = familyIndex ? summarizeSnapshot(familyIndex) : null;
  const summaryCards = familyIndex ? familyCardMetrics(familyIndex) : emptySummaryCards;
  const shortcutCards = data ? buildSummaryShortcuts(data.families) : [];

  const detailSummary = data
    ? filterValue.length === 0
      ? `${formatNumber(data.families.length)} families / ${formatNumber(sampleCount)} samples in the current snapshot.`
        : `${formatNumber(families.length)} of ${formatNumber(data.families.length)} families match “${filterValue}” with ${formatNumber(
          sampleRows.length,
        )} matching samples.`
    : "Searching metric families and samples for current counter values.";

  const headerStatus: { detail: string; label: string; tone: MetricsHeaderTone } = snapshotSummary
    ? {
        detail:
          filterValue.length === 0
            ? `${detailSummary} ${snapshotSummary.detail}`
            : `${detailSummary} ${snapshotSummary.nextStep}`,
        label: metrics.refreshing
          ? "Refreshing"
          : metrics.stale
            ? "Stale"
            : snapshotSummary.label,
        tone: metrics.refreshing
          ? "info"
          : metrics.stale
            ? "warning"
            : snapshotSummary.tone,
      }
    : {
        detail: "Searching metric families and samples from Prometheus response.",
        label: metrics.refreshing ? "Refreshing" : metrics.stale ? "Stale" : "Loading",
        tone: metrics.refreshing ? "info" : metrics.stale ? "warning" : "info",
      };

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Metrics inspection"
          title="Metrics explorer"
          description="Use the filters below to inspect live broker metric families and sample labels."
          primaryAction={{
            label: "Refresh metrics",
            onPress: () => metrics.refresh(),
          }}
          status={headerStatus}
        />

        {!data && metrics.loading ? (
          <QueryLoadingState title="Loading metrics snapshot" description="Reading the current /metrics payload." />
        ) : null}

        {!data && metrics.error ? (
          <QueryErrorState title="Unable to load metrics snapshot" error={metrics.error} onRetry={() => metrics.refresh()} />
        ) : null}

        <Stack gap="3">
          <section class="domain-section">
            <div class="domain-section-header">
              <div>
                <h2>Live state</h2>
                <p>The summary below reflects the full snapshot, even when the sample table is filtered.</p>
              </div>
            </div>
            <div class="chart-grid">
              <DomainMetricTable
                title="Broker snapshot"
                description="The broker process itself: uptime, connections, sessions, and routing pressure."
                metrics={summaryCards.broker}
              />

              <DomainMetricTable
                title="Delivery pressure"
                description="Where queued work and request/response load will show up first."
                metrics={summaryCards.delivery}
              />

              <DomainMetricTable
                title="Coordination state"
                description="Lease ownership, schedule claims, and stream append activity."
                metrics={summaryCards.coordination}
              />

              <DomainMetricTable
                title="Durable surfaces"
                description="The long-lived state and live fanout that make Fitz useful."
                metrics={summaryCards.state}
              />

              <DomainMetricTable
                title="Failure counters"
                description="These should stay flat; non-zero values usually need a closer look."
                metrics={summaryCards.failures}
              />
            </div>
          </section>

          <section class="domain-section">
            <div class="domain-section-header">
              <div>
                <h2>Search metrics</h2>
                <p>Filter by family name, then scan sample name, labels, and value in one compact table.</p>
              </div>
            </div>
            <div class="metrics-toolbar">
              <div class="metrics-filter">
                <Input
                  aria-label="Filter metrics"
                  placeholder="Search metric families"
                  type="search"
                  value={filter()}
                  onInput={(event: Event) => setFilter((event.target as HTMLInputElement).value)}
                />
              </div>

              <div class="metrics-shortcuts" role="group" aria-label="Metric family shortcuts">
                <For each={shortcutCards} by={(shortcut) => shortcut.label}>
                  {(shortcut) => (
                    <Button
                      size="sm"
                      class="metrics-shortcut"
                      variant={filterValue === shortcut.query ? "outline" : "ghost"}
                      onPress={() => setFilter(shortcut.query)}
                    >
                      {shortcut.label} ({formatNumber(shortcut.count)})
                    </Button>
                  )}
                </For>

                <For each={filterValue.length > 0 ? ["clear"] : []} by={() => "clear-filters"}>
                  {() => (
                    <Button
                      size="sm"
                      class="metrics-shortcut"
                      variant="ghost"
                      onPress={() => {
                        setFilter("");
                      }}
                    >
                      Clear filters
                    </Button>
                  )}
                </For>
              </div>
            </div>
          </section>

          <Card padding="sm" variant="default">
            <CardHeader>
              <CardTitle>Metric samples</CardTitle>
              <CardDescription>
                {data
                  ? families.length === 0
                    ? `${formatNumber(sampleRows.length)} visible samples`
                    : `Showing ${formatNumber(sampleRows.length)} of ${formatNumber(sampleCount)} samples`
                  : "Loading metric samples"}
              </CardDescription>
            </CardHeader>
            <CardContent>
              {data && sampleRows.length === 0 ? (
                <QueryEmptyState
                  title={filterValue.length > 0 ? "No matching metrics" : "No metrics available"}
                  description={
                    filterValue.length > 0
                      ? `No metric families match "${filterValue}". Use clear filters to return to the full table.`
                      : "No metric families were returned in this snapshot."
                  }
                />
              ) : (
                <div class="metrics-table-wrap">
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell class="metrics-family-header">Metric</TableHeaderCell>
                        <TableHeaderCell class="metrics-type-header">Type</TableHeaderCell>
                        <TableHeaderCell class="metrics-labels-header">Labels</TableHeaderCell>
                        <TableHeaderCell class="metrics-value-header">Value</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      <For each={sampleRows} by={(row) => `${row.family}:${row.labels}:${row.value}`}>
                        {(row) => (
                          <TableRow>
                            <TableCell>
                              <span class="metrics-family-cell" title={row.family}>
                                {row.family}
                              </span>
                            </TableCell>
                            <TableCell>{row.type}</TableCell>
                            <TableCell>
                              <code class="metrics-labels-cell" title={row.labels}>
                                {row.labels}
                              </code>
                            </TableCell>
                            <TableCell>
                              <code class="metrics-value-cell">{row.value}</code>
                            </TableCell>
                          </TableRow>
                        )}
                      </For>
                    </TableBody>
                  </Table>
                </div>
              )}
            </CardContent>
          </Card>

          {data ? (
            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <h2>Prometheus payload</h2>
                  <p>Exact broker payload for copy, diffing, and troubleshooting.</p>
                </div>
              </div>
              <pre class="resource-raw">{data.raw}</pre>
            </section>
          ) : null}
        </Stack>
      </Stack>
    </DomainPageFrame>
  );
}
