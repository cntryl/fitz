import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/components";
import { Inline, Stack } from "@askrjs/themes/components";
import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/components";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import type { DiagnosticHotspot, SuggestedQuery } from "@/adapters";
import {
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
  operatorLabel: string;
  system: SystemOverview;
  topology?: MessagingTopologyOverview | null;
  topologyError?: unknown;
  topologyLoading?: boolean;
}

interface InfrastructureRow {
  action: string;
  detail: string;
  signal: string;
  value: string | number;
}

interface DomainInternalRow {
  domain: string;
  failures: number;
  href: string;
  internals: string;
  pressure: number;
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
      signal: "Connections",
      value: system.broker.connections,
    },
    {
      action: "Review sessions",
      detail: "Authenticated sessions currently visible to admin.",
      signal: "Sessions",
      value: system.broker.sessions,
    },
    {
      action: "Review topology",
      detail: "Broker-local message throughput from global stats.",
      signal: "Messages / sec",
      value: fixedRate(system.broker.messagesPerSecond),
    },
    {
      action: "Review route families",
      detail: "Route-family groups from the topology snapshot.",
      signal: "Route families",
      value: topology?.sessionGroups.length ?? "Loading",
    },
    {
      action: "Review topology",
      detail: "Visible topology connections; truncated means more data exists server-side.",
      signal: "Topology connections",
      value: topology
        ? `${formatNumber(topology.connections.total)}${topology.connections.truncated ? " truncated" : ""}`
        : "Loading",
    },
    {
      action: "Open metrics",
      detail: "Parsed Prometheus metric families from the live metrics payload.",
      signal: "Metric families",
      value: metrics?.families.length ?? system.metrics.lineCount,
    },
    {
      action: "Review router pressure",
      detail: "Backpressure counters exposed by topology diagnostics.",
      signal: "Router backpressure",
      value: topology?.broker.routerBackpressureTotal ?? "Loading",
    },
  ];
}

function buildDomainRows(system: SystemOverview): DomainInternalRow[] {
  const domains = system.domains;

  return [
    {
      domain: "Queue",
      failures: domains.queue.messagesDeadLettered,
      href: domainHref("queue"),
      internals: `${formatNumber(domains.queue.messagesReady)} ready / ${formatNumber(
        domains.queue.inflightActive,
      )} inflight`,
      pressure:
        domains.queue.messagesPending +
        domains.queue.messagesReady +
        domains.queue.messagesDelayed +
        domains.queue.inflightActive,
    },
    {
      domain: "RPC",
      failures:
        domains.rpc.failureTotal +
        domains.rpc.requestTimeoutsTotal +
        domains.rpc.responsesDroppedClosedCallerTotal +
        domains.rpc.responsesMissingPendingTotal +
        domains.rpc.invalidSequenceErrorsDroppedTotal +
        domains.rpc.invalidSequenceErrorsForwardedTotal +
        domains.rpc.invalidSequenceResponsesTotal,
      href: domainHref("rpc"),
      internals: `${formatNumber(domains.rpc.workersRegistered)} workers / ${formatNumber(
        domains.rpc.requestsPending,
      )} pending`,
      pressure: domains.rpc.requestsPending + domains.rpc.pendingRoutesActive,
    },
    {
      domain: "Notice",
      failures:
        domains.notice.deliveryDropsTotal +
        domains.notice.failureTotal +
        domains.notice.wildcardLimitRejectsTotal,
      href: domainHref("notice"),
      internals: `${formatNumber(domains.notice.subscriptionsActive)} subscriptions`,
      pressure: domains.notice.subscriptionsActive,
    },
    {
      domain: "Schedule",
      failures:
        domains.schedule.ackFailuresTotal +
        domains.schedule.cancelPersistenceFailuresTotal +
        domains.schedule.createPersistenceFailuresTotal +
        domains.schedule.notifyFailuresTotal +
        domains.schedule.overdueNormalizationsTotal +
        domains.schedule.upsertPersistenceFailuresTotal,
      href: domainHref("schedule"),
      internals: `${formatNumber(domains.schedule.schedulesActive)} schedules / ${domains.schedule.executionsPerMinute.toFixed(
        2,
      )} exec/min`,
      pressure: domains.schedule.pendingFireClaims + domains.schedule.subscriptionsActive,
    },
    {
      domain: "Stream",
      failures:
        domains.stream.appendConflictsTotal +
        domains.stream.failureTotal +
        domains.stream.notifyDropsTotal,
      href: domainHref("stream"),
      internals: `${formatNumber(domains.stream.streamsActive)} streams / ${formatNumber(
        domains.stream.eventsTotal,
      )} events`,
      pressure: domains.stream.subscriptionsActive,
    },
    {
      domain: "Lease",
      failures:
        domains.lease.acquireTimeoutsTotal +
        domains.lease.failureTotal +
        domains.lease.forcedReleasesTotal +
        domains.lease.invalidTokenRejectsTotal,
      href: domainHref("lease"),
      internals: `${formatNumber(domains.lease.leasesActive)} active / oldest ${formatDurationSeconds(
        domains.lease.oldestLeaseAgeSeconds,
      )}`,
      pressure: domains.lease.waiterDepth,
    },
    {
      domain: "KV",
      failures: domains.kv.commitsFailedTotal + domains.kv.invalidTransactionRejectsTotal,
      href: domainHref("kv"),
      internals: `${formatNumber(domains.kv.keysTotal)} keys / ${formatNumber(
        domains.kv.transactionsActive,
      )} tx`,
      pressure: domains.kv.transactionsActive,
    },
  ];
}

const capabilityRows: CapabilityRow[] = [
  {
    capability: "Prometheus metrics",
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
  operatorLabel,
  system,
  topology,
  topologyError,
  topologyLoading = false,
}: DiagnosticsConsoleProps) {
  const incident = topology?.diagnostics.incident_summary ?? system.diagnostics.incident_summary;
  const incidentTone = severityTone(incident.severity);
  const infrastructureRows = buildInfrastructureRows(system, topology, metrics);
  const domainRows = buildDomainRows(system);
  const hotspots = hotspotsFrom(system, topology);
  const suggestedQueries = suggestedQueriesFrom(system, topology);
  const familyRows = metricFamilyRows(metrics);
  const infrastructureColumns: readonly VirtualTableColumn<InfrastructureRow>[] = [
    {
      id: "signal",
      header: "Signal",
      width: "22%",
      cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.signal}</span>,
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
      cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.action}</span>,
    },
  ];
  const domainColumns: readonly VirtualTableColumn<DomainInternalRow>[] = [
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
      id: "pressure",
      header: "Pressure",
      width: "18%",
      cellComponent: ({ row }) => <span>{formatNumber(row.pressure)}</span>,
    },
    {
      id: "failures",
      header: "Failures",
      width: "18%",
      cellComponent: ({ row }) => (
        <Badge variant={row.failures > 0 ? "warning" : "success"}>
          {formatNumber(row.failures)}
        </Badge>
      ),
    },
    {
      id: "internals",
      header: "Internals",
      width: "46%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.internals}>
          {row.internals}
        </span>
      ),
    },
  ];
  const hotspotColumns: readonly VirtualTableColumn<DiagnosticHotspot>[] = [
    {
      id: "scope",
      header: "Scope",
      width: "28%",
      cellComponent: ({ row }) => {
        const href = hotspotHref(row);
        const label = scopeText(row) || "Broker";

        return href ? (
          <Link class="text-link" href={href}>
            {label}
          </Link>
        ) : (
          <span class="domain-table-cell-truncate">{label}</span>
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
  const suggestedColumns: readonly VirtualTableColumn<SuggestedQuery>[] = [
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
        <code class="diagnostics-code-cell" title={row.endpoint}>
          {row.endpoint}
        </code>
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
  const metricColumns: readonly VirtualTableColumn<MetricFamilyRow>[] = [
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
    <Stack gap="3">
      <Card padding="sm" variant="default">
        <CardHeader>
          <Inline justify="between" align="start" gap="3" wrap="wrap">
            <Stack gap="1">
              <CardTitle>Diagnostics console</CardTitle>
              <CardDescription>
                Infrastructure internals for {operatorLabel}: topology pressure, suggested follow-up
                queries, metrics families, and exposed diagnostics.
              </CardDescription>
            </Stack>
            <Badge variant={badgeVariantForTone(incidentTone)}>{incident.severity}</Badge>
          </Inline>
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
              <strong>{incident.recommended_next_query ?? "No follow-up needed"}</strong>
              <span>Generated {formatTimestamp(topology?.generatedAt ?? system.fetchedAt)}</span>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card padding="sm" variant="default">
        <CardHeader>
          <CardTitle>Infrastructure signals</CardTitle>
          <CardDescription>
            Broker internals, topology shape, route-family groups, router pressure, and metrics
            coverage.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <VirtualTable<InfrastructureRow>
            aria-label="Infrastructure diagnostic signals"
            class="diagnostics-virtual-table"
            columns={infrastructureColumns}
            getKey={(row) => row.signal}
            headerHeight={44}
            overscan={3}
            rowHeight={48}
            rows={infrastructureRows}
            style={{ height: "320px" }}
          />
        </CardContent>
      </Card>

      <Card padding="sm" variant="default">
        <CardHeader>
          <CardTitle>Domain internals</CardTitle>
          <CardDescription>
            Cross-domain pressure and failure counters stay here so operational workflows remain
            focused on state, flow, failures, and actions.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <VirtualTable<DomainInternalRow>
            aria-label="Domain internal diagnostic counters"
            class="diagnostics-virtual-table"
            columns={domainColumns}
            getKey={(row) => row.domain}
            headerHeight={44}
            overscan={4}
            rowHeight={48}
            rows={domainRows}
            style={{ height: "360px" }}
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
                <Inline justify="between" align="start" gap="2">
                  <strong>{row.capability}</strong>
                  <Badge variant={row.status === "Existing API" ? "success" : "outline"}>
                    {row.status}
                  </Badge>
                </Inline>
                <p class="domain-muted">{row.owner}</p>
                <p>{row.why}</p>
              </div>
            )}
          </For>
        </div>
      </section>

      <Card padding="sm" variant="default">
        <CardHeader>
          <CardTitle>Hotspots</CardTitle>
          <CardDescription>
            Broker-generated diagnostic hotspots from the current topology or global stats snapshot.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {topologyLoading ? (
            <QueryLoadingState description="Loading topology hotspots..." />
          ) : null}
          {topologyError ? (
            <QueryErrorState title="Unable to load topology diagnostics" error={topologyError} />
          ) : null}
          {!topologyLoading && !topologyError ? (
            hotspots.length === 0 ? (
              <QueryEmptyState
                title="No diagnostic hotspots"
                description="The broker did not report pressure hotspots in this snapshot."
              />
            ) : (
              <VirtualTable<DiagnosticHotspot>
                aria-label="Diagnostic hotspots"
                class="diagnostics-virtual-table"
                columns={hotspotColumns}
                getKey={hotspotKey}
                headerHeight={44}
                overscan={4}
                rowHeight={48}
                rows={hotspots}
                style={{ height: "320px" }}
              />
            )
          ) : null}
        </CardContent>
      </Card>

      <Card padding="sm" variant="default">
        <CardHeader>
          <CardTitle>Suggested queries</CardTitle>
          <CardDescription>
            Backend-provided follow-up queries and remediation hints. These are diagnostics
            pointers, not domain workflow replacements.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {suggestedQueries.length === 0 ? (
            <QueryEmptyState
              title="No suggested queries"
              description="The broker did not recommend a follow-up query for this snapshot."
            />
          ) : (
            <VirtualTable<SuggestedQuery>
              aria-label="Suggested diagnostic queries"
              class="diagnostics-virtual-table"
              columns={suggestedColumns}
              getKey={suggestedQueryKey}
              headerHeight={44}
              overscan={4}
              rowHeight={48}
              rows={suggestedQueries}
              style={{ height: "320px" }}
            />
          )}
        </CardContent>
      </Card>

      <Card padding="sm" variant="default">
        <CardHeader>
          <Inline justify="between" align="start" gap="3" wrap="wrap">
            <Stack gap="1">
              <CardTitle>Metric families</CardTitle>
              <CardDescription>
                Parsed Prometheus families for storage-adjacent, routing, pressure, and failure
                diagnostics.
              </CardDescription>
            </Stack>
            <Button asChild size="sm" variant="outline">
              <Link href={adminChildHref("metrics")}>Open metrics explorer</Link>
            </Button>
          </Inline>
        </CardHeader>
        <CardContent>
          {metricsLoading ? <QueryLoadingState description="Loading metric families..." /> : null}
          {metricsError ? (
            <QueryErrorState title="Unable to load metric families" error={metricsError} />
          ) : null}
          {!metricsLoading && !metricsError ? (
            familyRows.length === 0 ? (
              <QueryEmptyState
                title="No metric families"
                description="The metrics endpoint did not return parseable metric families."
              />
            ) : (
              <VirtualTable<MetricFamilyRow>
                aria-label="Diagnostic metric families"
                class="diagnostics-virtual-table"
                columns={metricColumns}
                getKey={(row) => row.name}
                headerHeight={44}
                overscan={8}
                rowHeight={48}
                rows={familyRows}
                style={{ height: "384px" }}
              />
            )
          ) : null}
        </CardContent>
      </Card>
    </Stack>
  );
}
