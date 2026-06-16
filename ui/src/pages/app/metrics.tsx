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
import { Stack } from "@askrjs/themes/layouts";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import type { PrometheusMetricFamily } from "@/features/metrics/metrics-models";
import { createMetricsOverviewQuery } from "@/features/metrics/metrics-query";
import { formatNumber } from "@/shared/format";

function buildFamilyIndex(families: PrometheusMetricFamily[]) {
  return new Map(families.map((family) => [family.name, family]));
}

function familyValue(index: Map<string, PrometheusMetricFamily>, name: string) {
  return (
    index.get(name)?.samples.reduce((sum, sample) => sum + sample.value, 0) ?? 0
  );
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

function summarizeSnapshot(index: Map<string, PrometheusMetricFamily>) {
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
      detail: `Watch ${activeFailures.slice(0, 3).map((signal) => signalText(signal.label, signal.value)).join(", ")}.`,
      label: "Pressure",
      tone: "danger" as const,
    };
  }

  const activePressure = pressureSignals.filter((signal) => signal.value > 0);
  if (activePressure.length > 0) {
    return {
      detail: `Active pressure: ${activePressure.slice(0, 3).map((signal) => signalText(signal.label, signal.value)).join(", ")}.`,
      label: "Pressure",
      tone: "warning" as const,
    };
  }

  return {
    detail: "No backlog, contention, or failure pressure detected.",
    label: "Quiet",
    tone: "success" as const,
  };
}

function familyMetrics(family?: PrometheusMetricFamily) {
  return family?.samples.reduce((sum, sample) => sum + sample.value, 0) ?? 0;
}

function familyCardMetrics(index: Map<string, PrometheusMetricFamily>) {
  return {
    broker: [
      { label: "Uptime", value: familyMetrics(index.get("fitz_uptime_seconds")), caption: "seconds" },
      {
        label: "Connections",
        value: familyMetrics(index.get("fitz_connections_total")),
        caption: "open",
      },
      {
        label: "Sessions",
        value: familyMetrics(index.get("fitz_sessions_total")),
        caption: "active",
      },
      {
        label: "Messages received",
        value: familyMetrics(index.get("fitz_messages_received_total")),
        caption: "lifetime total",
      },
      {
        label: "Messages sent",
        value: familyMetrics(index.get("fitz_messages_sent_total")),
        caption: "lifetime total",
      },
      {
        label: "Router backpressure",
        value: familyMetrics(index.get("fitz_router_backpressure_total")),
        caption: "drops",
      },
      {
        label: "High-lane backpressure",
        value: familyMetrics(index.get("fitz_router_high_lane_backpressure_total")),
        caption: "drops",
      },
    ],
    delivery: [
      {
        label: "Queue ready",
        value: familyMetrics(index.get("fitz_queue_ready_gauge")),
        caption: "messages",
      },
      {
        label: "Queue inflight",
        value: familyMetrics(index.get("fitz_queue_inflight_active")),
        caption: "messages",
      },
      {
        label: "Queue pending",
        value: familyMetrics(index.get("fitz_queue_messages_pending")),
        caption: "messages",
      },
      {
        label: "Queue delayed",
        value: familyMetrics(index.get("fitz_queue_delayed_gauge")),
        caption: "messages",
      },
      {
        label: "Queue oldest message age",
        value: familyMetrics(index.get("fitz_queue_oldest_message_age_seconds")),
        caption: "seconds",
      },
      {
        label: "Queue backlog age",
        value: familyMetrics(index.get("fitz_queue_oldest_backlog_age_seconds")),
        caption: "seconds",
      },
      {
        label: "RPC workers",
        value: familyMetrics(index.get("fitz_rpc_workers_registered")),
        caption: "registered",
      },
      {
        label: "RPC pending",
        value: familyMetrics(index.get("fitz_rpc_requests_pending")),
        caption: "requests",
      },
      {
        label: "RPC oldest pending age",
        value: familyMetrics(index.get("fitz_rpc_oldest_pending_request_age_seconds")),
        caption: "seconds",
      },
    ],
    coordination: [
      {
        label: "Lease active",
        value: familyMetrics(index.get("fitz_lease_active")),
        caption: "claims",
      },
      {
        label: "Lease waiters",
        value: familyMetrics(index.get("fitz_lease_waiter_depth")),
        caption: "waiters",
      },
      {
        label: "Lease oldest age",
        value: familyMetrics(index.get("fitz_lease_oldest_lease_age_seconds")),
        caption: "seconds",
      },
      {
        label: "Schedule active",
        value: familyMetrics(index.get("fitz_schedule_active")),
        caption: "jobs",
      },
      {
        label: "Schedule pending claims",
        value: familyMetrics(index.get("fitz_schedule_pending_fire_claims")),
        caption: "claims",
      },
      {
        label: "Schedule ack retries",
        value: familyMetrics(index.get("fitz_schedule_pending_ack_retries")),
        caption: "retries",
      },
      {
        label: "Stream append sessions",
        value: familyMetrics(index.get("fitz_stream_append_sessions_active")),
        caption: "sessions",
      },
      {
        label: "Stream subscriptions",
        value: familyMetrics(index.get("fitz_stream_subscriptions_active")),
        caption: "subscriptions",
      },
    ],
    state: [
      {
        label: "KV keys",
        value: familyMetrics(index.get("fitz_kv_keys_total")),
        caption: "keys",
      },
      {
        label: "KV transactions",
        value: familyMetrics(index.get("fitz_kv_transactions_active")),
        caption: "active",
      },
      {
        label: "Notice subscriptions",
        value: familyMetrics(index.get("fitz_notice_subscriptions_active")),
        caption: "subscriptions",
      },
      {
        label: "Notice routes",
        value: familyMetrics(index.get("fitz_notice_routes_active")),
        caption: "routes",
      },
      {
        label: "Notice peak subscribers",
        value: familyMetrics(index.get("fitz_notice_max_route_subscribers")),
        caption: "peak",
      },
      {
        label: "Stream active",
        value: familyMetrics(index.get("fitz_stream_active")),
        caption: "streams",
      },
      {
        label: "Stream events",
        value: familyMetrics(index.get("fitz_stream_events_total")),
        caption: "committed",
      },
      {
        label: "Schedule executions / min",
        value: familyMetrics(index.get("fitz_schedule_executions_per_minute")).toFixed(2),
        caption: "per minute",
      },
    ],
    failures: [
      {
        label: "Queue redeliveries",
        value: familyMetrics(index.get("fitz_queue_redeliveries_total")),
        caption: "events",
      },
      {
        label: "Queue notify drops",
        value: familyMetrics(index.get("fitz_queue_notify_drops_total")),
        caption: "drops",
      },
      {
        label: "RPC backpressure rejects",
        value: familyMetrics(index.get("fitz_rpc_backpressure_rejects_total")),
        caption: "drops",
      },
      {
        label: "RPC request timeouts",
        value: familyMetrics(index.get("fitz_rpc_request_timeouts_total")),
        caption: "timeouts",
      },
      {
        label: "RPC missing pending",
        value: familyMetrics(index.get("fitz_rpc_responses_missing_pending_total")),
        caption: "responses",
      },
      {
        label: "Lease acquire timeouts",
        value: familyMetrics(index.get("fitz_lease_acquire_timeouts_total")),
        caption: "timeouts",
      },
      {
        label: "Lease forced releases",
        value: familyMetrics(index.get("fitz_lease_forced_releases_total")),
        caption: "releases",
      },
      {
        label: "Lease invalid tokens",
        value: familyMetrics(index.get("fitz_lease_invalid_token_rejects_total")),
        caption: "rejects",
      },
      {
        label: "Notice delivery drops",
        value: familyMetrics(index.get("fitz_notice_delivery_drops_total")),
        caption: "drops",
      },
      {
        label: "Notice wildcard rejects",
        value: familyMetrics(index.get("fitz_notice_wildcard_limit_rejects_total")),
        caption: "rejects",
      },
      {
        label: "Schedule notify failures",
        value: familyMetrics(index.get("fitz_schedule_notify_failures_total")),
        caption: "failures",
      },
      {
        label: "Schedule ack failures",
        value: familyMetrics(index.get("fitz_schedule_ack_failures_total")),
        caption: "failures",
      },
      {
        label: "Stream notify drops",
        value: familyMetrics(index.get("fitz_stream_notify_drops_total")),
        caption: "drops",
      },
      {
        label: "KV commit failures",
        value: familyMetrics(index.get("fitz_kv_commits_failed_total")),
        caption: "failures",
      },
      {
        label: "KV rollbacks",
        value: familyMetrics(index.get("fitz_kv_rollbacks_total")),
        caption: "rollbacks",
      },
    ],
  };
}

type MetricsHeaderTone = "default" | "info" | "success" | "warning" | "danger";

export default function MetricsPage() {
  const metrics = createMetricsOverviewQuery();
  const [filter, setFilter] = state("");
  const data = metrics.data;
  const filterValue = filter();
  const familyIndex = data ? buildFamilyIndex(data.families) : null;
  const families =
    data?.families.filter((family) =>
      family.name.toLowerCase().includes(filterValue.trim().toLowerCase()),
    ) ?? [];
  const sampleCount = data?.families.reduce((sum, family) => sum + family.samples.length, 0) ?? 0;
  const snapshotSummary = familyIndex ? summarizeSnapshot(familyIndex) : null;
  const summaryCards = familyIndex ? familyCardMetrics(familyIndex) : null;
  const headerStatus: { detail: string; label: string; tone: MetricsHeaderTone } = snapshotSummary
    ? {
        detail: `${formatNumber(data?.families.length ?? 0)} families / ${formatNumber(sampleCount)} samples. ${snapshotSummary.detail}`,
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
        detail: "Search the metric families and inspect the raw payload below.",
        label: metrics.refreshing ? "Refreshing" : metrics.stale ? "Stale" : "Loading",
        tone: metrics.refreshing ? "info" : metrics.stale ? "warning" : "info",
      };

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Metrics inspection"
          title="Metrics explorer"
          description="Read the live broker state first, then drill into the exact Prometheus families behind it."
          primaryAction={{
            label: "Refresh metrics",
            onPress: () => metrics.refresh(),
          }}
          status={headerStatus}
        />

        {!data && metrics.loading ? (
          <QueryLoadingState description="Loading Prometheus metrics..." />
        ) : null}

        {!data && metrics.error ? (
          <QueryErrorState error={metrics.error} onRetry={() => metrics.refresh()} />
        ) : null}

        {data ? (
          <Stack gap="3">
            {metrics.refreshing ? (
              <QueryRefreshingState description="Refreshing Prometheus metrics..." />
            ) : null}

            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <h2>Live state</h2>
                  <p>
                    The summary below turns raw metric families into the broker state picture.
                    It stays unfiltered even when the table search is narrowed.
                  </p>
                </div>
              </div>

              {summaryCards ? (
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
              ) : null}
            </section>

            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <h2>Search metrics</h2>
                  <p>
                    Filter by family name to drill into the table below. The state summary stays
                    focused on the live broker snapshot.
                  </p>
                </div>
              </div>
              <div class="auth-field metrics-filter">
                <Input
                  aria-label="Filter metrics"
                  placeholder="Search metrics"
                  value={filterValue}
                  onInput={(event: Event) => setFilter((event.target as HTMLInputElement).value)}
                />
              </div>
            </section>

            <Card class="metrics-family-card" padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Metric families</CardTitle>
                <CardDescription>
                  {families.length} visible of {data.families.length} families
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div class="domain-table-wrap">
                  <Table class="domain-table">
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Name</TableHeaderCell>
                        <TableHeaderCell>Type</TableHeaderCell>
                        <TableHeaderCell>Samples</TableHeaderCell>
                        <TableHeaderCell>Help</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {families.map((family) => (
                        <TableRow key={family.name}>
                          <TableCell>{family.name}</TableCell>
                          <TableCell>{family.type ?? "unknown"}</TableCell>
                          <TableCell>{family.samples.length}</TableCell>
                          <TableCell>{family.help ?? "n/a"}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>

            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <h2>Prometheus payload</h2>
                  <p>Exact broker payload for copy, diffing, and troubleshooting.</p>
                </div>
              </div>
              <pre class="resource-raw">{data.raw}</pre>
            </section>
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
