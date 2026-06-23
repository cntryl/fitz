import { For } from "@askrjs/askr/control";
import { Timeline } from "@askrjs/charts/components";
import { Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import { Button } from "@askrjs/themes/controls";
import { Flex, Stack } from "@askrjs/themes/layouts";
import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import { formatDurationSeconds } from "@/shared/format";
import DomainMetricTable from "./domain-metric-table";
import { QueryEmptyState } from "./query-state";
import type { ResourceDetail, ResourceRelatedTable } from "@/features/resource/resource-models";

export interface ResourceWorkbenchProps {
  detail: ResourceDetail;
}

export interface ResourceWorkbenchState {
  detail: string;
  label: string;
  nextStep: string;
  tone: "default" | "info" | "success" | "warning" | "danger";
}

function summarizeComparison(summary: string) {
  const normalized = summary.toLowerCase();

  if (
    normalized.includes("match") ||
    normalized.includes("same") ||
    normalized.includes("identical") ||
    normalized.includes("no material difference")
  ) {
    return {
      label: "Matched",
      tone: "success" as const,
    };
  }

  if (
    normalized.includes("diff") ||
    normalized.includes("change") ||
    normalized.includes("drift") ||
    normalized.includes("behind") ||
    normalized.includes("ahead") ||
    normalized.includes("mismatch")
  ) {
    return {
      label: "Changed",
      tone: "warning" as const,
    };
  }

  return {
    label: "Compared",
    tone: "info" as const,
  };
}

function humanizeAge(ageSeconds: number | null | undefined) {
  if (ageSeconds == null) {
    return "Unknown";
  }

  return formatDurationSeconds(ageSeconds);
}

function formatComparisonScope(scope?: { area: string; realm: string; resource: string }) {
  return scope ? `${scope.realm} / ${scope.area} / ${scope.resource}` : "Unknown";
}

function scopeLine(ref: { area: string; realm: string; resource: string }) {
  return `${ref.realm} / ${ref.area} / ${ref.resource}`;
}

function formatTimelineKind(kind: string) {
  switch (kind) {
    case "failure":
      return "Failure";
    case "retry":
      return "Retry";
    case "ownership_change":
      return "Ownership change";
    case "state_flip":
      return "State flip";
    case "registration":
      return "Registration";
    case "transition":
      return "Transition";
    default:
      return "Observation";
  }
}

function describeResourceState(detail: ResourceDetail): ResourceWorkbenchState {
  const { domain, detailMetrics } = detail;
  const latestEvent = detail.timeline.events[0];
  const eventSentence = latestEvent ? ` Latest event: ${latestEvent.summary}.` : "";

  if (detail.comparison) {
    const comparison = summarizeComparison(detail.comparison.summary);
    const comparedText = `Compared with target scope ${formatComparisonScope(
      detail.comparison.rightScope,
    )} using ${detail.comparison.comparisonMode}.`;

    return {
      detail: `${comparedText} ${detail.comparison.summary}.${eventSentence}`,
      label: comparison.label,
      nextStep:
        "Review the comparison summary, then inspect timeline and related records for operational context.",
      tone: comparison.tone,
    };
  }

  const latestLabel = detail.timeline.events.length
    ? `${detail.timeline.events.length} recent event${detail.timeline.events.length === 1 ? "" : "s"}`
    : "No recent timeline entries";

  if (detail.timeline.derived) {
    return {
      detail: `${latestLabel} for derived ${domain.toUpperCase()} evidence in scope ${scopeLine(
        detail.ref,
      )}.${eventSentence}`,
      label: "Derived",
      nextStep: "Open timeline and related tables to confirm live behavior details.",
      tone: "info",
    };
  }

  const highSignalMetric = detailMetrics.find(
    (metric) =>
      metric.label.toLowerCase().includes("diagnostic severity") ||
      metric.label.toLowerCase().includes("critical") ||
      metric.label.toLowerCase().includes("active") ||
      metric.label.toLowerCase().includes("operations"),
  );

  const signalText = highSignalMetric
    ? `${highSignalMetric.label}: ${highSignalMetric.value}.`
    : "Live scope values are present.";

  return {
    detail: `${latestLabel} in scope ${scopeLine(detail.ref)}. ${signalText}${eventSentence}`,
    label: "Live",
    nextStep: "Review timeline, related records, and raw payload for exact values.",
    tone: "success",
  };
}

function ComparisonDetails({
  comparison,
  scope,
}: {
  comparison: NonNullable<ResourceWorkbenchProps["detail"]["comparison"]>;
  scope: string;
}) {
  const summaryTone = summarizeComparison(comparison.summary);

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <Flex justify="between" gap="3" align="start" wrap="wrap">
          <Stack gap="1">
            <CardTitle>Comparison summary</CardTitle>
            <CardDescription>
              Current scope: {scope}. Target scope: {formatComparisonScope(comparison?.rightScope)}.
            </CardDescription>
          </Stack>
          <Badge variant={comparison.derived ? "info" : "success"}>
            {comparison.derived ? "Derived" : "Live"}
          </Badge>
        </Flex>
      </CardHeader>
      <CardContent>
        <DomainMetricTable
          title="Difference"
          description={`Compared using ${comparison.comparisonMode}. ${comparison.summary}`}
          metrics={[
            ...comparison.metrics,
            {
              label: "Result",
              value: summaryTone.label,
            },
          ]}
        />
      </CardContent>
    </Card>
  );
}

function RelatedTable({ table }: { table: ResourceRelatedTable }) {
  const columns: readonly VirtualTableColumn<Record<string, string | number>>[] = table.columns.map(
    (column) => ({
      id: column,
      header: column,
      width: `${100 / Math.max(table.columns.length, 1)}%`,
      cellComponent: ({ row }) => (
        <span class="resource-table-cell-truncate" title={String(row[column] ?? "n/a")}>
          {row[column] ?? "n/a"}
        </span>
      ),
    }),
  );
  const tableHeight = Math.min(432, Math.max(144, 44 + table.rows.length * 48));

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>{table.title}</CardTitle>
        <CardDescription>
          {table.rows.length} row{table.rows.length === 1 ? "" : "s"}
        </CardDescription>
      </CardHeader>
      <CardContent>
        <VirtualTable<Record<string, string | number>>
          aria-label={table.title}
          class="resource-related-virtual-table"
          columns={columns}
          getKey={(_row, index) => index}
          headerHeight={44}
          overscan={8}
          rowHeight={48}
          rows={table.rows}
          style={{ height: `${tableHeight}px` }}
        />
      </CardContent>
    </Card>
  );
}

export function describeResourceDetail(detail: ResourceDetail): ResourceWorkbenchState {
  return describeResourceState(detail);
}

type ResourceArchetypeConfig = {
  actionLabel: string;
  actionTitle: string;
  diagnosticsDescription: string;
  evidenceTitle: string;
  failureTitle: string;
  primaryDescription: string;
  primaryTitle: string;
  timelineTitle: string;
  title: string;
};

const archetypeConfig: Record<ResourceDetail["domain"], ResourceArchetypeConfig> = {
  kv: {
    actionLabel: "State Explorer",
    actionTitle: "Query workspace",
    diagnosticsDescription: "Transaction pressure, current values, and raw resource payload.",
    evidenceTitle: "Results and details",
    failureTitle: "State anomalies",
    primaryDescription: "Resource-level current authoritative state from the existing admin API.",
    primaryTitle: "State query",
    timelineTitle: "State timeline",
    title: "KV State Explorer",
  },
  lease: {
    actionLabel: "Ownership Console",
    actionTitle: "Ownership",
    diagnosticsDescription: "Broker-local lease health and contention evidence.",
    evidenceTitle: "Contention",
    failureTitle: "Ownership conflicts",
    primaryDescription: "Ephemeral owner, waiter, and lease coordination signals for this scope.",
    primaryTitle: "Current ownership",
    timelineTitle: "Ownership history",
    title: "Lease Ownership Console",
  },
  notice: {
    actionLabel: "Communication Flow",
    actionTitle: "Flow graph",
    diagnosticsDescription: "Live fanout pressure, participants, failures, and raw broker evidence.",
    evidenceTitle: "Participants",
    failureTitle: "Delivery failures",
    primaryDescription: "Live Notice route, subscription, and delivery signals for connected participants.",
    primaryTitle: "Notice flow",
    timelineTitle: "Delivery trace",
    title: "Notice Communication Flow",
  },
  rpc: {
    actionLabel: "Communication Flow",
    actionTitle: "Flow graph",
    diagnosticsDescription: "Live request/response participants, failures, and pending-call evidence.",
    evidenceTitle: "Participants",
    failureTitle: "Call failures",
    primaryDescription: "Live RPC operations, workers, and pending request signals for this resource.",
    primaryTitle: "RPC flow",
    timelineTitle: "Call trace",
    title: "RPC Communication Flow",
  },
  schedule: {
    actionLabel: "Time Planner",
    actionTitle: "Timeline",
    diagnosticsDescription: "Durable timing intent, execution pressure, and handoff diagnostics.",
    evidenceTitle: "Executions",
    failureTitle: "Missed or failed execution",
    primaryDescription: "Future timing intent and recent execution evidence for this schedule resource.",
    primaryTitle: "Execution plan",
    timelineTitle: "Execution timeline",
    title: "Schedule Time Planner",
  },
  stream: {
    actionLabel: "History Explorer",
    actionTitle: "Event explorer",
    diagnosticsDescription: "Durable stream indicators, consumers, replay context, and raw payload.",
    evidenceTitle: "Consumers",
    failureTitle: "Replay risks",
    primaryDescription: "Durable history indicators and recent stream events for this scope.",
    primaryTitle: "Event history",
    timelineTitle: "Event timeline",
    title: "Stream History Explorer",
  },
};

function failureLikeMetric(metric: { label: string }) {
  const label = metric.label.toLowerCase();

  return [
    "fail",
    "reject",
    "timeout",
    "drop",
    "dead",
    "conflict",
    "invalid",
    "rollback",
  ].some((word) => label.includes(word));
}

function failureLikeEvent(event: { kind: string; summary: string }) {
  const text = `${event.kind} ${event.summary}`.toLowerCase();

  return [
    "fail",
    "reject",
    "timeout",
    "drop",
    "dead",
    "conflict",
    "invalid",
    "blocked",
  ].some((word) => text.includes(word));
}

function hierarchyMetrics(detail: ResourceDetail) {
  return [
    { label: "Realm", value: detail.ref.realm },
    { label: "Area", value: detail.ref.area },
    { label: "Resource", value: detail.ref.resource },
    ...detail.detailMetrics.slice(0, 5),
  ];
}

function ResourceTimelinePanel({
  detail,
  title,
}: {
  detail: ResourceDetail;
  title: string;
}) {
  const timelineData = detail.timeline.events.slice(0, 8).map((event) => ({
    description: event.summary,
    label: formatTimelineKind(event.kind),
    value: humanizeAge(event.ageSeconds),
  }));

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <Flex justify="between" gap="3" align="start" wrap="wrap">
          <Stack gap="1">
            <CardTitle>{title}</CardTitle>
            <CardDescription>
              {detail.timeline.derived ? "Derived evidence" : "Live evidence"} for{" "}
              {scopeLine(detail.ref)}.
            </CardDescription>
          </Stack>
          <Badge variant={detail.timeline.derived ? "info" : "success"}>
            {detail.timeline.derived ? "Derived" : "Live"}
          </Badge>
        </Flex>
      </CardHeader>
      <CardContent>
        {timelineData.length > 0 ? (
          <Timeline
            label={title}
            data={timelineData}
            summary={`${timelineData.length} recent timeline event(s).`}
          />
        ) : (
          <QueryEmptyState
            title="No timeline events"
            description="No recent events are visible for this resource."
          />
        )}
      </CardContent>
    </Card>
  );
}

function FailurePanel({
  detail,
  title,
}: {
  detail: ResourceDetail;
  title: string;
}) {
  const failureMetrics = detail.detailMetrics.filter(failureLikeMetric);
  const failureEvents = detail.timeline.events.filter(failureLikeEvent).slice(0, 5);

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>Failures, rejects, drops, conflicts, and other attention signals.</CardDescription>
      </CardHeader>
      <CardContent>
        {failureMetrics.length > 0 || failureEvents.length > 0 ? (
          <Stack gap="3">
            {failureMetrics.length > 0 ? (
              <DomainMetricTable title="Failure metrics" metrics={failureMetrics} />
            ) : null}
            {failureEvents.length > 0 ? (
              <div class="domain-table-wrap">
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableHeaderCell>Kind</TableHeaderCell>
                      <TableHeaderCell>Summary</TableHeaderCell>
                      <TableHeaderCell>Observed</TableHeaderCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    <For
                      each={failureEvents}
                      by={(event) => `${event.observedAt}:${event.summary}`}
                    >
                      {(event) => (
                        <TableRow>
                          <TableCell>{formatTimelineKind(event.kind)}</TableCell>
                          <TableCell>{event.summary}</TableCell>
                          <TableCell>{event.observedAt}</TableCell>
                        </TableRow>
                      )}
                    </For>
                  </TableBody>
                </Table>
              </div>
            ) : null}
          </Stack>
        ) : (
          <QueryEmptyState
            title="No failure signals"
            description="No failure, reject, drop, conflict, or timeout signals are visible in the current admin data."
          />
        )}
      </CardContent>
    </Card>
  );
}

function ArchetypeActionPanel({
  config,
  detail,
}: {
  config: ResourceArchetypeConfig;
  detail: ResourceDetail;
}) {
  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>{config.actionTitle}</CardTitle>
        <CardDescription>{config.primaryDescription}</CardDescription>
      </CardHeader>
      <CardContent>
        <DomainMetricTable
          title={config.primaryTitle}
          description={`Scope: ${scopeLine(detail.ref)}.`}
          metrics={hierarchyMetrics(detail)}
        />
      </CardContent>
    </Card>
  );
}

function ArchetypeEvidencePanel({
  config,
  detail,
}: {
  config: ResourceArchetypeConfig;
  detail: ResourceDetail;
}) {
  const related = detail.related.filter((table) => table.rows.length > 0);

  return (
    <Stack gap="3">
      <Card padding="sm" variant="default">
        <CardHeader>
          <CardTitle>{config.evidenceTitle}</CardTitle>
          <CardDescription>{config.diagnosticsDescription}</CardDescription>
        </CardHeader>
        <CardContent>
          <DomainMetricTable title="Current values" metrics={detail.detailMetrics} />
        </CardContent>
      </Card>

      {detail.comparison ? (
        <ComparisonDetails comparison={detail.comparison} scope={scopeLine(detail.ref)} />
      ) : null}

      {related.length > 0 ? (
        <For each={related} by={(table) => table.title}>
          {(table) => <RelatedTable table={table} />}
        </For>
      ) : (
        <QueryEmptyState
          title="No related records"
          description="No related records are visible for this scope."
        />
      )}
    </Stack>
  );
}

function ArchetypeOperationsPanel({ detail }: { detail: ResourceDetail }) {
  if (detail.domain === "stream") {
    return (
      <Card padding="sm" variant="default">
        <CardHeader>
          <CardTitle>Replay controls</CardTitle>
          <CardDescription>Replay remains tied to explicit Stream API support.</CardDescription>
        </CardHeader>
        <CardContent>
          <Button variant="outline" disabled>
            Replay event range
          </Button>
        </CardContent>
      </Card>
    );
  }

  if (detail.domain === "kv") {
    return (
      <Card padding="sm" variant="default">
        <CardHeader>
          <CardTitle>State lookup</CardTitle>
          <CardDescription>Key lookup and prefix search require a dedicated KV admin contract.</CardDescription>
        </CardHeader>
        <CardContent>
          <Button variant="outline" disabled>
            Query keys
          </Button>
        </CardContent>
      </Card>
    );
  }

  return null;
}

export default function ResourceWorkbench({ detail }: ResourceWorkbenchProps) {
  const summary = describeResourceDetail(detail);
  const config = archetypeConfig[detail.domain];

  return (
    <Stack gap="4" class="resource-workbench archetype-workbench">
      <Card class="resource-workbench-hero" padding="sm" variant="default">
        <Stack gap="1" class="resource-workbench-summary">
          <p class="domain-header-kicker">{config.actionLabel}</p>
          <h2>{config.title}</h2>
          <p class="domain-muted">{summary.nextStep}</p>
        </Stack>
        <Stack gap="2" class="resource-workbench-summary-actions">
          <Badge variant={summary.tone}>{summary.label}</Badge>
          <Link href={`/${detail.domain}`}>Back to {detail.domain} overview</Link>
        </Stack>
      </Card>

      <ArchetypeActionPanel config={config} detail={detail} />
      <ResourceTimelinePanel detail={detail} title={config.timelineTitle} />
      <ArchetypeEvidencePanel config={config} detail={detail} />
      <FailurePanel detail={detail} title={config.failureTitle} />
      <ArchetypeOperationsPanel detail={detail} />

      <section class="resource-workbench-raw">
        <Card padding="sm" variant="default">
          <CardHeader>
            <CardTitle>Diagnostics payload</CardTitle>
            <CardDescription>Exact API response body for this resource.</CardDescription>
          </CardHeader>
          <CardContent>
            <pre class="resource-raw">{JSON.stringify(detail.raw, null, 2)}</pre>
          </CardContent>
        </Card>
      </section>
    </Stack>
  );
}
