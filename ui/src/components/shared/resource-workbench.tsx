import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
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

function formatTimelineContext(event: {
  attempts?: number | null;
  area: string;
  correlationId?: string | null;
  messageId?: number | null;
  operation?: string | null;
  ownerSession?: string | null;
  realm: string;
  resource: string;
  workerSession?: string | null;
}): string[] {
  return [
    `Scope: ${event.realm} / ${event.area} / ${event.resource}`,
    event.operation ? `Operation: ${event.operation}` : null,
    event.messageId != null ? `Message: ${event.messageId}` : null,
    event.attempts != null ? `Attempts: ${event.attempts}` : null,
    event.ownerSession ? `Owner session: ${event.ownerSession}` : null,
    event.workerSession ? `Worker session: ${event.workerSession}` : null,
    event.correlationId ? `Correlation: ${event.correlationId}` : null,
  ].filter((line): line is string => line != null);
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
  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>{table.title}</CardTitle>
        <CardDescription>
          {table.rows.length} row{table.rows.length === 1 ? "" : "s"}
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div class="domain-table-wrap">
          <Table>
            <TableHead>
              <TableRow>
                <For each={table.columns} by={(column) => column}>
                  {(column) => <TableHeaderCell>{column}</TableHeaderCell>}
                </For>
              </TableRow>
            </TableHead>
            <TableBody>
              <For each={table.rows} by={(_row, index) => index}>
                {(row) => (
                  <TableRow>
                    <For each={table.columns} by={(column) => column}>
                      {(column) => (
                        <TableCell>
                          <span class="resource-table-cell-truncate">{row[column] ?? "n/a"}</span>
                        </TableCell>
                      )}
                    </For>
                  </TableRow>
                )}
              </For>
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}

export function describeResourceDetail(detail: ResourceDetail): ResourceWorkbenchState {
  return describeResourceState(detail);
}

export default function ResourceWorkbench({ detail }: ResourceWorkbenchProps) {
  const summary = describeResourceDetail(detail);
  const timelineScope = scopeLine(detail.timeline);
  const hasMeaningfulRelated = detail.related.filter((table) => table.rows.length > 0).length > 0;

  return (
    <Stack gap="4" class="resource-workbench">
      <Card class="resource-workbench-hero" padding="sm" variant="default">
        <Stack gap="1" class="resource-workbench-summary">
          <p class="domain-header-kicker">Operational evidence</p>
          <h2>{detail.timeline.derived ? "Derived signal available" : "Live signal available"}</h2>
          <p class="domain-muted">{summary.nextStep}</p>
        </Stack>
        <Stack gap="2" class="resource-workbench-summary-actions">
          <Badge variant={summary.tone}>{summary.label}</Badge>
          <Link href={`/${detail.domain}`}>Back to {detail.domain} overview</Link>
        </Stack>
      </Card>

      <DomainMetricTable
        title="Current values"
        description={`Primary metrics for ${scopeLine(detail.ref)}.`}
        metrics={detail.detailMetrics}
      />

      <div class="resource-comparison-metadata">
        {detail.comparison ? (
          <ComparisonDetails comparison={detail.comparison} scope={scopeLine(detail.ref)} />
        ) : null}
        {!detail.comparison ? (
          <QueryEmptyState
            title="Add a comparison"
            description="Enter realm, area, and resource in the sidebar to compare this scope against another scope."
          />
        ) : null}
      </div>

      <Card padding="sm" variant="default">
        <CardHeader>
          <Flex justify="between" gap="3" align="start" wrap="wrap">
            <Stack gap="1">
              <CardTitle>Timeline</CardTitle>
              <CardDescription>
                {detail.timeline.derived
                  ? "Derived timeline built from surrounding evidence."
                  : "Live events observed for this scope."}
                <span class="resource-muted-meta">
                  {" "}
                  Limit {detail.timeline.limit} rows. Scope: {timelineScope}.
                </span>
              </CardDescription>
            </Stack>
            <Badge variant={detail.timeline.derived ? "info" : "success"}>
              {detail.timeline.derived ? "Derived" : "Live"}
            </Badge>
          </Flex>
        </CardHeader>

        <CardContent>
          {detail.timeline.events.length === 0 ? (
            <QueryEmptyState
              title={detail.timeline.derived ? "Derived timeline" : "Live timeline"}
              description="No recent events are visible for this resource. Use the current metrics or raw payload for exact values."
            />
          ) : (
            <div class="domain-table-wrap">
              <Table>
                <TableHead>
                  <TableRow>
                    <TableHeaderCell class="resource-timeline-kind">Kind</TableHeaderCell>
                    <TableHeaderCell>Summary</TableHeaderCell>
                    <TableHeaderCell>Context</TableHeaderCell>
                    <TableHeaderCell>Observed</TableHeaderCell>
                    <TableHeaderCell>Age</TableHeaderCell>
                  </TableRow>
                </TableHead>

                <TableBody>
                  <For
                    each={detail.timeline.events}
                    by={(event) => `${event.observedAt}:${event.summary}`}
                  >
                    {(event) => {
                      const timelineContext = formatTimelineContext(event);

                      return (
                        <TableRow>
                          <TableCell>
                            <span class="resource-timeline-kind">
                              {formatTimelineKind(event.kind)}
                            </span>
                          </TableCell>
                          <TableCell>
                            <span class="resource-timeline-summary">{event.summary}</span>
                          </TableCell>
                          <TableCell>
                            <div class="resource-timeline-context">
                              {timelineContext.length > 0 ? (
                                timelineContext.map((line) => <span>{line}</span>)
                              ) : (
                                <span>No context</span>
                              )}
                            </div>
                          </TableCell>
                          <TableCell>{event.observedAt}</TableCell>
                          <TableCell>{humanizeAge(event.ageSeconds)}</TableCell>
                        </TableRow>
                      );
                    }}
                  </For>
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {hasMeaningfulRelated ? (
        <Stack gap="3">
          <For each={detail.related} by={(table) => table.title}>
            {(table) => (table.rows.length > 0 ? <RelatedTable table={table} /> : null)}
          </For>
        </Stack>
      ) : (
        <QueryEmptyState
          title="No related records"
          description="No related records are visible for this scope."
        />
      )}

      <section class="resource-workbench-raw">
        <Card padding="sm" variant="default">
          <CardHeader>
            <CardTitle>Raw payload</CardTitle>
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
