import { For, Show } from "@askrjs/askr/control";
import { currentRoute, updateRouteQuery } from "@askrjs/askr/router";
import { Input } from "@askrjs/ui";
import { Button, Text, Block } from "@askrjs/themes/components";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DataTable, { type DataTableColumn } from "@/components/shared/data-table";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryCompactEmptyState,
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import type { MetricFamily } from "@/features/metrics/metrics-models";
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
      return /(fail|reject|drop|timeout|rollback|invalid|wrong|missing|error)/.test(name);
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

function buildFamilyIndex(families: MetricFamily[]) {
  return new Map(families.map((family) => [family.name, family]));
}

function observedFamilyValue(index: Map<string, MetricFamily>, name: string) {
  const family = index.get(name);

  return family ? family.samples.reduce((sum, sample) => sum + sample.value, 0) : null;
}

function familyValue(index: Map<string, MetricFamily>, name: string) {
  return observedFamilyValue(index, name) ?? "--";
}

function fixedFamilyValue(index: Map<string, MetricFamily>, name: string) {
  const value = observedFamilyValue(index, name);

  return value === null ? "--" : value.toFixed(2);
}

function signalText(label: string, value: number) {
  return `${label} ${formatNumber(value)}`;
}

function summarizeSnapshot(index: Map<string, MetricFamily>): MetricsPostureSummary {
  const deadLetters = observedFamilyValue(index, "fitz_queue_messages_dead_lettered");
  const activitySignals = [
    { label: "queue pending", name: "fitz_queue_messages_pending" },
    { label: "queue inflight", name: "fitz_queue_inflight_active" },
    { label: "RPC pending", name: "fitz_rpc_requests_pending" },
    { label: "lease waiters", name: "fitz_lease_waiter_depth" },
    { label: "schedule claims", name: "fitz_schedule_pending_fire_claims" },
    { label: "schedule ack retries", name: "fitz_schedule_pending_ack_retries" },
    { label: "stream append sessions", name: "fitz_stream_append_sessions_active" },
    { label: "KV transactions", name: "fitz_kv_transactions_active" },
    { label: "notice subscriptions", name: "fitz_notice_subscriptions_active" },
  ].map((signal) => ({ ...signal, value: observedFamilyValue(index, signal.name) }));

  if (deadLetters !== null && deadLetters > 0) {
    return {
      detail: `${signalText("queue dead letters", deadLetters)} currently require a replay or purge decision. Cumulative failure counters below describe process history, not active incidents.`,
      label: "Attention",
      nextStep: "Open Queue and inspect the current dead-letter set.",
      tone: "danger" as const,
    };
  }

  const missingSignals = activitySignals.filter((signal) => signal.value === null);
  if (missingSignals.length > 0 || deadLetters === null) {
    return {
      detail: `${formatNumber(activitySignals.length - missingSignals.length)} of ${formatNumber(activitySignals.length)} current-activity families are available. Missing telemetry is not treated as zero.`,
      label: "Incomplete",
      nextStep:
        "Refresh the snapshot, then inspect the missing metric families before judging health.",
      tone: "warning" as const,
    };
  }

  const activeSignals = activitySignals.flatMap((signal) =>
    signal.value !== null && signal.value > 0 ? [{ label: signal.label, value: signal.value }] : [],
  );
  if (activeSignals.length > 0) {
    return {
      detail: `${activeSignals.length} current activity signal${activeSignals.length === 1 ? " is" : "s are"} non-zero: ${activeSignals
        .slice(0, 3)
        .map((signal) => signalText(signal.label, signal.value))
        .join(", ")}. Activity alone does not establish pressure.`,
      label: "Active",
      nextStep:
        "Use age, lag, and broker diagnostics to decide whether active work needs attention.",
      tone: "info" as const,
    };
  }

  return {
    detail:
      "All observed current-activity gauges are zero. Cumulative counters below remain historical process totals.",
    label: "Quiet",
    nextStep:
      "Use the search box to inspect a specific metric family when you need a narrower read.",
    tone: "success",
  };
}

function familyCardMetrics(index: Map<string, MetricFamily>) {
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
        value: fixedFamilyValue(index, "fitz_schedule_executions_per_minute"),
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
    ],
  };
}

function familyNameMatches(familyName: string, query: string) {
  return query.length === 0 || familyName.toLowerCase().includes(query);
}

function formatLabels(labels: Record<string, string>) {
  const entries = Object.entries(labels).sort(([left], [right]) => left.localeCompare(right));

  if (entries.length === 0) {
    return "Unknown";
  }

  return entries.map(([key, value]) => `${key}="${value}"`).join(", ");
}

function buildRows(families: MetricFamily[], filterValue: string): MetricsSampleRow[] {
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

function buildSummaryShortcuts(families: MetricFamily[]) {
  return metricsShortcuts
    .map((shortcut) => ({
      label: shortcut.label,
      query: shortcut.query,
      count: families.filter((family) => shortcut.test(family.name.toLowerCase())).length,
      test: shortcut.test,
    }))
    .filter((shortcut) => shortcut.count > 0);
}

function MetricsShortcuts({
  filterValue,
  setFilter,
  shortcutCards,
}: {
  filterValue: string;
  setFilter: (value: string) => void;
  shortcutCards: ReturnType<typeof buildSummaryShortcuts>;
}) {
  return (
    <div class="metrics-shortcuts" role="group" aria-label="Metric family shortcuts">
      <For each={shortcutCards} by={(shortcut) => shortcut.label}>
        {(shortcut) => (
          <Button
            size="sm"
            class="metrics-shortcut"
            variant={filterValue === shortcut.query ? "outline" : "ghost"}
            aria-pressed={filterValue === shortcut.query}
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
              queueMicrotask(() => document.getElementById("metrics-filter")?.focus());
            }}
          >
            Clear filters
          </Button>
        )}
      </For>
    </div>
  );
}

export default function MetricsPage() {
  const metrics = createMetricsOverviewQuery();
  const filter = () => currentRoute().query.get("q") ?? "";
  const setFilter = (value: string) => updateRouteQuery({ q: value || null });
  const data = metrics.data;
  const filterValue = filter().trim().toLowerCase();
  const familyIndex = data ? buildFamilyIndex(data.families) : null;
  const families =
    data?.families.filter((family) => familyNameMatches(family.name.toLowerCase(), filterValue)) ??
    [];
  const sampleRows = data ? buildRows(data.families, filterValue) : [];
  const sampleCount = data?.families.reduce((sum, family) => sum + family.samples.length, 0) ?? 0;
  const snapshotSummary = familyIndex ? summarizeSnapshot(familyIndex) : null;
  const rawSummaryCards = familyIndex ? familyCardMetrics(familyIndex) : emptySummaryCards;
  const summaryCards = {
    broker: rawSummaryCards.broker.filter((metric) => metric.value !== "--"),
    coordination: rawSummaryCards.coordination.filter((metric) => metric.value !== "--"),
    delivery: rawSummaryCards.delivery.filter((metric) => metric.value !== "--"),
    failures: rawSummaryCards.failures.filter((metric) => metric.value !== "--"),
    state: rawSummaryCards.state.filter((metric) => metric.value !== "--"),
  };
  const summaryMetricCount = Object.values(summaryCards).reduce(
    (sum, metricsForCategory) => sum + metricsForCategory.length,
    0,
  );
  const shortcutCards = data ? buildSummaryShortcuts(data.families) : [];
  const sampleColumns: readonly DataTableColumn<MetricsSampleRow>[] = [
    {
      id: "metric",
      header: "Metric",
      width: "34%",
      cellComponent: ({ row }) => (
        <span class="metrics-family-cell" title={row.family}>
          {row.family}
        </span>
      ),
    },
    {
      id: "type",
      header: "Type",
      width: "12%",
      cellComponent: ({ row }) => <span>{row.type}</span>,
    },
    {
      id: "labels",
      header: "Labels",
      width: "38%",
      cellComponent: ({ row }) => (
        <code class="metrics-labels-cell" title={row.labels}>
          {row.labels}
        </code>
      ),
    },
    {
      id: "value",
      header: "Value",
      width: "16%",
      cellComponent: ({ row }) => (
        <Text
          as="span"
          class="metrics-value-cell"
          font="mono"
          numeric="tabular"
          size="sm"
          title={String(row.value)}
          weight="medium"
        >
          {row.value}
        </Text>
      ),
    },
  ];
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
        label: metrics.refreshing ? "Refreshing" : metrics.stale ? "Stale" : snapshotSummary.label,
        tone: metrics.refreshing ? "info" : metrics.stale ? "warning" : snapshotSummary.tone,
      }
    : metrics.error
      ? {
          detail: "The structured metrics snapshot is unavailable.",
          label: "Unavailable",
          tone: "danger",
        }
      : {
          detail: "Searching metric families and samples from structured broker metrics.",
          label: metrics.refreshing ? "Refreshing" : metrics.stale ? "Stale" : "Loading",
          tone: metrics.refreshing ? "info" : metrics.stale ? "warning" : "info",
        };

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          eyebrow="Metrics inspection"
          title="Metrics explorer"
          description="Use the filters below to inspect live broker metric families and sample labels."
          primaryAction={{
            busy: metrics.refreshing,
            disabled: metrics.refreshing,
            label: "Refresh metrics",
            onPress: () => metrics.refresh(),
          }}
          status={headerStatus}
        />

        <Show when={!data && metrics.loading}>
          <QueryLoadingState
            title="Loading metrics snapshot"
            description="Reading the current structured route-family metrics snapshot."
          />
        </Show>

        <Show when={!data && metrics.error}>
          <QueryErrorState
            title="Unable to load metrics snapshot"
            error={metrics.error}
            onRetry={() => metrics.refresh()}
          />
        </Show>

        <Show when={data}>
          {(data) => (
            <Block direction="column" gap="sm">
              <section class="domain-section">
                <div class="domain-section-header">
                  <div>
                    <h2>Live state</h2>
                    <p>
                      The summary below reflects the full snapshot, even when the sample table is
                      filtered.
                    </p>
                  </div>
                </div>
                <div class="chart-grid">
                  {summaryCards.broker.length > 0 ? (
                    <DomainMetricTable
                      title="Broker snapshot"
                      titleAs="h3"
                      description="The broker process itself: uptime, connections, sessions, and cumulative routing counters."
                      metrics={summaryCards.broker}
                    />
                  ) : null}

                  {summaryCards.delivery.length > 0 ? (
                    <DomainMetricTable
                      title="Delivery activity"
                      titleAs="h3"
                      description="Current queued work and request/response activity. Non-zero values are not pressure by themselves."
                      metrics={summaryCards.delivery}
                    />
                  ) : null}

                  {summaryCards.coordination.length > 0 ? (
                    <DomainMetricTable
                      title="Coordination state"
                      titleAs="h3"
                      description="Lease ownership, schedule claims, and stream append activity."
                      metrics={summaryCards.coordination}
                    />
                  ) : null}

                  {summaryCards.state.length > 0 ? (
                    <DomainMetricTable
                      title="Durable surfaces"
                      titleAs="h3"
                      description="The long-lived state and live fanout that make Fitz useful."
                      metrics={summaryCards.state}
                    />
                  ) : null}

                  {summaryCards.failures.length > 0 ? (
                    <DomainMetricTable
                      title="Failure counters"
                      titleAs="h3"
                      description="Cumulative totals since the broker process started. Inspect changes between snapshots before treating them as active failures."
                      metrics={summaryCards.failures}
                    />
                  ) : null}
                </div>
                {summaryMetricCount === 0 ? (
                  <QueryCompactEmptyState
                    title="No known summary metrics"
                    description="The snapshot has samples, but none match the summary gauges on this page. Use the sample filter below."
                  />
                ) : null}
              </section>

              <Card padding="sm" variant="default">
                <CardHeader>
                  <CardTitle titleAs="h2">Metric samples</CardTitle>
                  <CardDescription role="status" aria-live="polite" aria-atomic="true">
                    {data
                      ? families.length === 0
                        ? `${formatNumber(sampleRows.length)} visible samples`
                        : `Showing ${formatNumber(sampleRows.length)} of ${formatNumber(sampleCount)} samples`
                      : "Loading metric samples"}
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div class="metrics-toolbar">
                    <div class="metrics-filter">
                      <Input
                        id="metrics-filter"
                        aria-label="Filter metrics"
                        placeholder="Search metric families"
                        type="search"
                        value={filter()}
                        onInput={(event: Event) =>
                          setFilter((event.target as HTMLInputElement).value)
                        }
                      />
                    </div>

                    <MetricsShortcuts
                      filterValue={filterValue}
                      setFilter={setFilter}
                      shortcutCards={shortcutCards}
                    />
                  </div>
                  {data && sampleRows.length === 0 ? (
                    <QueryEmptyState
                      title={
                        filterValue.length > 0 ? "No matching metrics" : "No metrics available"
                      }
                      description={
                        filterValue.length > 0
                          ? `No metric families match "${filterValue}". Use clear filters to return to the full table.`
                          : "No metric families were returned in this snapshot."
                      }
                    />
                  ) : (
                    <DataTable<MetricsSampleRow>
                      ariaLabel="Metric samples"
                      class="metrics-sample-data-table"
                      columns={sampleColumns}
                      getKey={(row) => `${row.family}:${row.labels}:${row.value}`}
                      rows={sampleRows}
                    />
                  )}
                </CardContent>
              </Card>

              <section class="domain-section metrics-structured-payload">
                <Collapsible>
                  <div class="domain-section-header">
                    <div>
                      <h2>Structured payload</h2>
                      <p>
                        Exact route-family JSON snapshot for copy, diffing, and troubleshooting.
                      </p>
                    </div>
                    <CollapsibleTrigger asChild>
                      <Button variant="outline">View structured payload</Button>
                    </CollapsibleTrigger>
                  </div>
                  <CollapsibleContent>
                    <pre class="resource-raw">{data.raw}</pre>
                  </CollapsibleContent>
                </Collapsible>
              </section>
            </Block>
          )}
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
