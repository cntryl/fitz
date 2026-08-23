import { For, Show } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Button, Block } from "@askrjs/themes/components";
import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/components";
import DataTable, { type DataTableColumn } from "@/components/shared/data-table";
import type { DiagnosticHotspot, SuggestedQuery } from "@/adapters";
import {
  QueryCompactEmptyState,
  QueryEmptyState,
  QueryErrorState,
  QueryLoadingState,
} from "@/components/shared/query-state";
import type { MetricsOverview } from "@/features/metrics/metrics-models";
import type { SystemOverview } from "@/features/system/system-models";
import type { MessagingTopologyOverview } from "@/features/topology/topology-models";
import { hotspotHref, scopeText } from "@/features/topology/topology-view";
import { formatDurationSeconds, formatNumber, formatTimestamp } from "@/shared/format";
import { adminChildHref, domainHref } from "@/shared/navigation/domains";

type DiagnosticsTone = "danger" | "warning" | "success" | "info";

interface DiagnosticsConsoleProps {
  metrics?: MetricsOverview | null;
  metricsError?: unknown;
  metricsLoading?: boolean;
  onRetryMetrics?: () => void;
  onRetryTopology?: () => void;
  operatorLabel: string;
  system: SystemOverview;
  topology?: MessagingTopologyOverview | null;
  topologyError?: unknown;
  topologyLoading?: boolean;
}

interface InfrastructureRow {
  action: string;
  detail: string;
  href: string;
  signal: string;
  value: string | number;
}

interface DomainInternalRow {
  activity: number;
  cumulativeFailures: number | null;
  domain: string;
  href: string;
  internals: string;
}

interface CapabilityRow {
  capability: string;
  owner: string;
  status: "Existing API" | "Not exposed";
  why: string;
}

interface MetricFamilyRow {
  help: string;
  name: string;
  samples: number;
  type: string;
}

function severityTone(severity: string | undefined): DiagnosticsTone {
  if (severity === "critical" || severity === "high") return "danger";
  if (severity === "medium" || severity === "low") return "warning";
  if (severity === "informational") return "success";

  return "info";
}

function badgeVariantForTone(tone: DiagnosticsTone) {
  if (tone === "danger") return "danger";
  if (tone === "warning") return "warning";
  if (tone === "success") return "success";

  return "info";
}

function fixedRate(value: number) {
  return `${value.toFixed(2)} / sec`;
}

function buildInfrastructureRows(
  system: SystemOverview,
  topology?: MessagingTopologyOverview | null,
  metrics?: MetricsOverview | null,
): InfrastructureRow[] {
  return [
    {
      action: "Review sessions",
      detail: "Open broker connections in the current admin snapshot.",
      href: adminChildHref("sessions"),
      signal: "Connections",
      value: system.broker.connections,
    },
    {
      action: "Review sessions",
      detail: "Authenticated sessions currently visible to admin.",
      href: adminChildHref("sessions"),
      signal: "Sessions",
      value: system.broker.sessions,
    },
    {
      action: "Review topology",
      detail: "Broker-local message throughput from global stats.",
      href: `${adminChildHref("diagnostics")}#diagnostic-hotspots`,
      signal: "Messages / sec",
      value: fixedRate(system.broker.messagesPerSecond),
    },
    {
      action: "Review Route Families",
      detail: "Route-family groups from the topology snapshot.",
      href: "/",
      signal: "Route families",
      value: topology?.sessionGroups.length ?? "Loading",
    },
    {
      action: "Review topology",
      detail: "Visible topology connections; truncated means more data exists server-side.",
      href: `${adminChildHref("diagnostics")}#diagnostic-hotspots`,
      signal: "Topology connections",
      value: topology
        ? `${formatNumber(topology.connections.total)}${topology.connections.truncated ? " truncated" : ""}`
        : "Loading",
    },
    {
      action: "Open metrics",
      detail: "Structured metric families from the live route-family metrics snapshot.",
      href: adminChildHref("metrics"),
      signal: "Metric families",
      value: metrics?.families.length ?? system.metrics.lineCount,
    },
    {
      action: "Review router pressure",
      detail: "Cumulative backpressure total exposed by topology diagnostics.",
      href: `${adminChildHref("diagnostics")}#diagnostic-hotspots`,
      signal: "Router backpressure total",
      value: topology?.broker.routerBackpressureTotal ?? "Loading",
    },
  ];
}

function buildDomainRows(system: SystemOverview): DomainInternalRow[] {
  const domains = system.domains;

  return [
    {
      activity:
        domains.queue.messagesPending +
        domains.queue.messagesReady +
        domains.queue.messagesDelayed +
        domains.queue.inflightActive,
      cumulativeFailures: null,
      domain: "Queue",
      href: domainHref("queue"),
      internals: `${formatNumber(domains.queue.messagesReady)} ready / ${formatNumber(
        domains.queue.inflightActive,
      )} inflight / ${formatNumber(domains.queue.messagesDeadLettered)} dead-lettered`,
    },
    {
      activity: domains.rpc.requestsPending + domains.rpc.pendingRoutesActive,
      cumulativeFailures:
        domains.rpc.failureTotal +
        domains.rpc.requestTimeoutsTotal +
        domains.rpc.responsesDroppedClosedCallerTotal +
        domains.rpc.responsesMissingPendingTotal +
        domains.rpc.invalidSequenceErrorsDroppedTotal +
        domains.rpc.invalidSequenceErrorsForwardedTotal +
        domains.rpc.invalidSequenceResponsesTotal,
      domain: "RPC",
      href: domainHref("rpc"),
      internals: `${formatNumber(domains.rpc.workersRegistered)} workers / ${formatNumber(
        domains.rpc.requestsPending,
      )} pending`,
    },
    {
      activity: domains.notice.subscriptionsActive,
      cumulativeFailures:
        domains.notice.deliveryDropsTotal +
        domains.notice.failureTotal +
        domains.notice.wildcardLimitRejectsTotal,
      domain: "Notice",
      href: domainHref("notice"),
      internals: `${formatNumber(domains.notice.subscriptionsActive)} subscriptions`,
    },
    {
      activity: domains.schedule.pendingFireClaims + domains.schedule.subscriptionsActive,
      cumulativeFailures:
        domains.schedule.ackFailuresTotal +
        domains.schedule.cancelPersistenceFailuresTotal +
        domains.schedule.createPersistenceFailuresTotal +
        domains.schedule.notifyFailuresTotal +
        domains.schedule.overdueNormalizationsTotal +
        domains.schedule.upsertPersistenceFailuresTotal,
      domain: "Schedule",
      href: domainHref("schedule"),
      internals: `${formatNumber(domains.schedule.schedulesActive)} schedules / ${domains.schedule.executionsPerMinute.toFixed(
        2,
      )} exec/min`,
    },
    {
      activity: domains.stream.subscriptionsActive,
      cumulativeFailures:
        domains.stream.appendConflictsTotal +
        domains.stream.failureTotal +
        domains.stream.notifyDropsTotal,
      domain: "Stream",
      href: domainHref("stream"),
      internals: `${formatNumber(domains.stream.streamsActive)} streams / ${formatNumber(
        domains.stream.eventsTotal,
      )} events`,
    },
    {
      activity: domains.lease.waiterDepth,
      cumulativeFailures:
        domains.lease.acquireTimeoutsTotal +
        domains.lease.failureTotal +
        domains.lease.forcedReleasesTotal +
        domains.lease.invalidTokenRejectsTotal,
      domain: "Lease",
      href: domainHref("lease"),
      internals: `${formatNumber(domains.lease.leasesActive)} active / oldest ${formatDurationSeconds(
        domains.lease.oldestLeaseAgeSeconds,
      )}`,
    },
    {
      activity: domains.kv.transactionsActive,
      cumulativeFailures: domains.kv.commitsFailedTotal + domains.kv.invalidTransactionRejectsTotal,
      domain: "KV",
      href: domainHref("kv"),
      internals: `${formatNumber(domains.kv.keysTotal)} keys / ${formatNumber(
        domains.kv.transactionsActive,
      )} tx`,
    },
  ];
}

const capabilityRows: CapabilityRow[] = [
  {
    capability: "Structured metrics",
    owner: "Broker",
    status: "Existing API",
    why: "Used for counter-level inspection, failure counters, and advanced metric sampling.",
  },
  {
    capability: "Messaging topology",
    owner: "Broker",
    status: "Existing API",
    why: "Used for hotspots, connection state, route-family groups, and current flow pressure.",
  },
  {
    capability: "Active sessions",
    owner: "Admin API",
    status: "Existing API",
    why: "Used for transport/session diagnostics without assigning durable ownership meaning.",
  },
  {
    capability: "Storage health",
    owner: "Storage layer",
    status: "Not exposed",
    why: "No current admin endpoint exposes storage device health or persistence lag as a scoped diagnostic view.",
  },
  {
    capability: "Replication",
    owner: "Storage layer",
    status: "Not exposed",
    why: "No current admin endpoint exposes replication topology, lag, or quorum state.",
  },
  {
    capability: "Checkpointing",
    owner: "Storage layer",
    status: "Not exposed",
    why: "No current admin endpoint exposes checkpoint age, checkpoint failures, or recovery window.",
  },
  {
    capability: "Compaction",
    owner: "Storage layer",
    status: "Not exposed",
    why: "No current admin endpoint exposes compaction backlog, segment pressure, or compaction failures.",
  },
];

function metricFamilyRows(metrics?: MetricsOverview | null): MetricFamilyRow[] {
  return (
    metrics?.families.map((family) => ({
      help: family.help ?? "No help text",
      name: family.name,
      samples: family.samples.length,
      type: family.type ?? "unknown",
    })) ?? []
  );
}

function suggestedQueriesFrom(system: SystemOverview, topology?: MessagingTopologyOverview | null) {
  const fromTopology = topology?.diagnostics.incident_summary.suggested_next_queries;

  return fromTopology && fromTopology.length > 0
    ? fromTopology
    : system.diagnostics.incident_summary.suggested_next_queries;
}

function hotspotsFrom(system: SystemOverview, topology?: MessagingTopologyOverview | null) {
  const fromTopology = topology?.diagnostics.hotspots;

  return fromTopology && fromTopology.length > 0 ? fromTopology : system.diagnostics.hotspots;
}

function suggestedQueryKey(query: SuggestedQuery) {
  return `${query.priority}:${query.endpoint}:${query.title}`;
}

function hotspotKey(hotspot: DiagnosticHotspot) {
  return [
    hotspot.domain,
    hotspot.realm ?? "any",
    hotspot.area ?? "any",
    hotspot.resource ?? "any",
    hotspot.current_stage,
  ].join(":");
}

export default function DiagnosticsConsole({
  metrics,
  metricsError,
  metricsLoading = false,
  onRetryMetrics,
  onRetryTopology,
  operatorLabel,
  system,
  topology,
  topologyError,
  topologyLoading = false,
}: DiagnosticsConsoleProps) {
  const incident = topology?.diagnostics.incident_summary ?? system.diagnostics.incident_summary;
  const incidentTone = severityTone(incident.severity);
  const recommendedNextQuery =
    incident.status === "healthy" || incident.severity === "informational"
      ? null
      : incident.recommended_next_query;
  const infrastructureRows = buildInfrastructureRows(system, topology, metrics);
  const domainRows = buildDomainRows(system);
  const hotspots = hotspotsFrom(system, topology);
  const suggestedQueries = suggestedQueriesFrom(system, topology);
  const familyRows = metricFamilyRows(metrics);
  const infrastructureColumns: readonly DataTableColumn<InfrastructureRow>[] = [
    {
      id: "signal",
      header: "Signal",
      width: "22%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.signal}>
          {row.signal}
        </span>
      ),
    },
    {
      id: "value",
      header: "Value",
      width: "18%",
      cellComponent: ({ row }) => <span class="diagnostics-value-cell">{row.value}</span>,
    },
    {
      id: "detail",
      header: "Diagnostic meaning",
      width: "38%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.detail}>
          {row.detail}
        </span>
      ),
    },
    {
      id: "action",
      header: "Action",
      width: "22%",
      cellComponent: ({ row }) => (
        <Link class="text-link" href={row.href}>
          {row.action}
        </Link>
      ),
    },
  ];
  const domainColumns: readonly DataTableColumn<DomainInternalRow>[] = [
    {
      id: "domain",
      header: "Domain",
      width: "18%",
      cellComponent: ({ row }) => (
        <Link class="text-link" href={row.href}>
          {row.domain}
        </Link>
      ),
    },
    {
      id: "activity",
      header: "Current activity",
      width: "18%",
      cellComponent: ({ row }) => <span>{formatNumber(row.activity)}</span>,
    },
    {
      id: "cumulative-failures",
      header: "Cumulative signals",
      width: "22%",
      cellComponent: ({ row }) => (
        <Badge variant="outline">
          {row.cumulativeFailures === null ? "--" : formatNumber(row.cumulativeFailures)}
        </Badge>
      ),
    },
    {
      id: "internals",
      header: "Internals",
      width: "42%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.internals}>
          {row.internals}
        </span>
      ),
    },
  ];
  const hotspotColumns: readonly DataTableColumn<DiagnosticHotspot>[] = [
    {
      id: "scope",
      header: "Route",
      width: "28%",
      cellComponent: ({ row }) => {
        const href = hotspotHref(row);
        const label = scopeText(row) || "Broker";

        return href ? (
          <Link class="text-link" href={href}>
            {label}
          </Link>
        ) : (
          <span class="domain-table-cell-truncate" title={label}>
            {label}
          </span>
        );
      },
    },
    {
      id: "severity",
      header: "Severity",
      width: "16%",
      cellComponent: ({ row }) => (
        <Badge variant={badgeVariantForTone(severityTone(row.severity))}>{row.severity}</Badge>
      ),
    },
    {
      id: "stage",
      header: "Stage",
      width: "22%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.current_stage}>
          {row.current_stage}
        </span>
      ),
    },
    {
      id: "evidence",
      header: "Evidence",
      width: "34%",
      cellComponent: ({ row }) => (
        <span
          class="domain-table-cell-truncate"
          title={row.explanation_hints[0] ?? row.likely_bottleneck ?? "Review diagnostics"}
        >
          {row.explanation_hints[0] ?? row.likely_bottleneck ?? "Review diagnostics"}
        </span>
      ),
    },
  ];
  const suggestedColumns: readonly DataTableColumn<SuggestedQuery>[] = [
    {
      id: "priority",
      header: "Priority",
      width: "14%",
      cellComponent: ({ row }) => <span>{row.priority}</span>,
    },
    {
      id: "title",
      header: "Query",
      width: "26%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.title}>
          {row.title}
        </span>
      ),
    },
    {
      id: "endpoint",
      header: "Endpoint",
      width: "26%",
      cellComponent: ({ row }) => (
        <a
          class="text-link diagnostics-code-cell"
          href={row.endpoint}
          target="_blank"
          rel="noreferrer"
        >
          <code title={row.endpoint}>{row.endpoint}</code>
        </a>
      ),
    },
    {
      id: "remediation",
      header: "Why",
      width: "34%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.remediation || row.rationale}>
          {row.remediation || row.rationale}
        </span>
      ),
    },
  ];
  const metricColumns: readonly DataTableColumn<MetricFamilyRow>[] = [
    {
      id: "name",
      header: "Family",
      width: "34%",
      cellComponent: ({ row }) => (
        <Link
          class="text-link"
          href={`${adminChildHref("metrics")}?q=${encodeURIComponent(row.name)}`}
        >
          {row.name}
        </Link>
      ),
    },
    {
      id: "type",
      header: "Type",
      width: "14%",
      cellComponent: ({ row }) => <span>{row.type}</span>,
    },
    {
      id: "samples",
      header: "Samples",
      width: "14%",
      cellComponent: ({ row }) => <span>{formatNumber(row.samples)}</span>,
    },
    {
      id: "help",
      header: "Help",
      width: "38%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.help}>
          {row.help}
        </span>
      ),
    },
  ];

  return (
    <Block direction="column" gap="sm">
      <Card padding="sm" variant="default">
        <CardHeader>
          <Block direction="row" justify="between" align="start" gap="sm" wrap={true}>
            <Block direction="column" gap="xs">
              <CardTitle titleAs="h2">Diagnostics console</CardTitle>
              <CardDescription>
                Infrastructure internals for {operatorLabel}: topology pressure, suggested follow-up
                queries, metrics families, and exposed diagnostics.
              </CardDescription>
            </Block>
            <Badge variant={badgeVariantForTone(incidentTone)}>{incident.severity}</Badge>
          </Block>
        </CardHeader>
        <CardContent>
          <div class="diagnostics-summary-grid">
            <div class="diagnostics-summary-card">
              <span class="domain-header-kicker">Incident</span>
              <strong>{incident.title}</strong>
              <span>{incident.explanation}</span>
            </div>
            <div class="diagnostics-summary-card">
              <span class="domain-header-kicker">Status</span>
              <strong>{incident.status}</strong>
              <span>Confidence {(incident.confidence * 100).toFixed(0)}%</span>
            </div>
            <div class="diagnostics-summary-card">
              <span class="domain-header-kicker">Next query</span>
              <strong>{recommendedNextQuery ?? "No follow-up needed"}</strong>
              <span>Generated {formatTimestamp(topology?.generatedAt ?? system.fetchedAt)}</span>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card padding="sm" variant="default">
        <CardHeader>
          <CardTitle titleAs="h2">Infrastructure signals</CardTitle>
          <CardDescription>
            Broker internals, topology shape, route-family groups, router pressure, and metrics
            coverage.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <DataTable<InfrastructureRow>
            ariaLabel="Infrastructure diagnostic signals"
            class="diagnostics-data-table"
            columns={infrastructureColumns}
            getKey={(row) => row.signal}
            rows={infrastructureRows}
          />
        </CardContent>
      </Card>

      <Card padding="sm" variant="default">
        <CardHeader>
          <CardTitle titleAs="h2">Domain internals</CardTitle>
          <CardDescription>
            Current activity and cumulative process counters stay separate so historical totals do
            not masquerade as active incidents.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <DataTable<DomainInternalRow>
            ariaLabel="Domain internal diagnostic counters"
            class="diagnostics-data-table"
            columns={domainColumns}
            getKey={(row) => row.domain}
            rows={domainRows}
          />
        </CardContent>
      </Card>

      <section class="domain-section">
        <div class="domain-section-header">
          <div>
            <h2>Advanced operational views</h2>
            <p>What Diagnostics can inspect today and what is not exposed by the admin API.</p>
          </div>
        </div>

        <div class="diagnostics-contract-grid">
          <For each={capabilityRows} by={(row) => row.capability}>
            {(row) => (
              <div class="diagnostics-contract-card">
                <Block direction="row" justify="between" align="start" gap="xs">
                  <strong>{row.capability}</strong>
                  <Badge variant={row.status === "Existing API" ? "success" : "outline"}>
                    {row.status}
                  </Badge>
                </Block>
                <p class="domain-muted">{row.owner}</p>
                <p>{row.why}</p>
              </div>
            )}
          </For>
        </div>
      </section>

      <Card id="diagnostic-hotspots" padding="sm" variant="default">
        <CardHeader>
          <CardTitle titleAs="h2">Hotspots</CardTitle>
          <CardDescription>
            Broker-generated diagnostic hotspots from the current topology or global stats snapshot.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Show when={topologyLoading}>
            <QueryLoadingState description="Loading topology hotspots..." />
          </Show>
          <Show when={topologyError}>
            <QueryErrorState
              title="Unable to load topology diagnostics"
              error={topologyError}
              onRetry={onRetryTopology}
            />
          </Show>
          <Show when={!topologyLoading && !topologyError}>
            <Show
              when={hotspots.length === 0}
              fallback={
                <DataTable<DiagnosticHotspot>
                  ariaLabel="Diagnostic hotspots"
                  class="diagnostics-data-table"
                  columns={hotspotColumns}
                  getKey={hotspotKey}
                  rows={hotspots}
                />
              }
            >
              <QueryEmptyState
                title="No diagnostic hotspots"
                description="The broker did not report pressure hotspots in this snapshot."
              />
            </Show>
          </Show>
        </CardContent>
      </Card>

      <Card padding="sm" variant="default">
        <CardHeader>
          <CardTitle titleAs="h2">Suggested queries</CardTitle>
          <CardDescription>
            Backend-provided follow-up queries and remediation hints. These are diagnostics
            pointers, not domain workflow replacements.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Show
            when={suggestedQueries.length === 0}
            fallback={
              <DataTable<SuggestedQuery>
                ariaLabel="Suggested diagnostic queries"
                class="diagnostics-data-table"
                columns={suggestedColumns}
                getKey={suggestedQueryKey}
                rows={suggestedQueries}
              />
            }
          >
            <QueryCompactEmptyState
              title="No suggested queries"
              description="The broker did not recommend a follow-up query for this snapshot."
            />
          </Show>
        </CardContent>
      </Card>

      <Card padding="sm" variant="default">
        <CardHeader>
          <Block direction="row" justify="between" align="start" gap="sm" wrap={true}>
            <Block direction="column" gap="xs">
              <CardTitle titleAs="h2">Metric families</CardTitle>
              <CardDescription>
                Structured metric families for storage-adjacent, routing, pressure, and failure
                diagnostics.
              </CardDescription>
            </Block>
            <Button asChild size="sm" variant="outline">
              <Link href={adminChildHref("metrics")}>Open metrics explorer</Link>
            </Button>
          </Block>
        </CardHeader>
        <CardContent>
          <Show when={metricsLoading}>
            <QueryLoadingState description="Loading metric families..." />
          </Show>
          <Show when={metricsError}>
            <QueryErrorState
              title="Unable to load metric families"
              error={metricsError}
              onRetry={onRetryMetrics}
            />
          </Show>
          <Show when={!metricsLoading && !metricsError}>
            <Show
              when={familyRows.length === 0}
              fallback={
                <DataTable<MetricFamilyRow>
                  ariaLabel="Diagnostic metric families"
                  class="diagnostics-data-table"
                  columns={metricColumns}
                  getKey={(row) => row.name}
                  rows={familyRows}
                />
              }
            >
              <QueryEmptyState
                title="No metric families"
                description="The metrics endpoint did not return parseable metric families."
              />
            </Show>
          </Show>
        </CardContent>
      </Card>
    </Block>
  );
}
