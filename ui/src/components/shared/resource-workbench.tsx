import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Flex, Stack } from "@askrjs/themes/layouts";
import { Badge, Card, CardContent, CardDescription, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
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

export function describeResourceDetail(detail: ResourceDetail): ResourceWorkbenchState {
  const latestEvent = detail.timeline.events[0];

  if (detail.comparison) {
    const comparison = summarizeComparison(detail.comparison.summary);
    const eventSentence = latestEvent ? ` Visible event: ${latestEvent.summary}.` : "";

    return {
      detail: `Compared against the selected ${detail.comparison.comparisonMode} scope. ${detail.comparison.summary}.${eventSentence}`,
      label: comparison.label,
      nextStep:
        "Use the comparison details below, then check the timeline, related records, and raw payload for context.",
      tone: comparison.tone,
    };
  }

  if (detail.timeline.events.length === 0) {
    return {
      detail:
        "No recent events are visible. Use the related tables and raw payload if you need exact values.",
      label: detail.timeline.derived ? "Derived" : "Quiet",
      nextStep: "Use the current snapshot and raw payload for exact values.",
      tone: detail.timeline.derived ? "info" : "success",
    };
  }

  const sourceLabel = detail.timeline.derived ? "Derived" : "Live";
  const eventSentence = latestEvent ? ` Visible event: ${latestEvent.summary}.` : "";

  return {
    detail: `${sourceLabel} timeline with ${detail.timeline.events.length} visible event${
      detail.timeline.events.length === 1 ? "" : "s"
    }.${eventSentence}`,
    label: sourceLabel,
    nextStep: "Review the timeline, related records, and raw payload below.",
    tone: detail.timeline.derived ? "info" : "success",
  };
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
        {table.rows.length === 0 ? (
          <QueryEmptyState
            title="No related rows"
            description={`No rows are visible in ${table.title}.`}
          />
        ) : (
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
                        {(column) => <TableCell>{row[column] ?? "n/a"}</TableCell>}
                      </For>
                    </TableRow>
                  )}
                </For>
              </TableBody>
            </Table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

export default function ResourceWorkbench({ detail }: ResourceWorkbenchProps) {
  const summary = describeResourceDetail(detail);

  return (
    <Stack gap="4" class="resource-workbench">
      <Card class="resource-workbench-hero" padding="sm" variant="default">
        <Stack gap="1" class="resource-workbench-summary">
          <p class="domain-header-kicker">Operational evidence</p>
          <h2>Where to inspect next</h2>
          <p class="domain-muted">{summary.nextStep}</p>
        </Stack>
        <Stack gap="2" class="resource-workbench-summary-actions">
          <Badge variant={summary.tone}>{summary.label}</Badge>
          <Link href={`/${detail.domain}`}>Back to {detail.domain} overview</Link>
        </Stack>
      </Card>

      <DomainMetricTable
        title="Current values"
        description="Exact values for the current resource."
        metrics={detail.detailMetrics}
      />

      {detail.comparison ? (
        <DomainMetricTable
          title="Comparison details"
          description={`Compared against the selected ${detail.comparison.comparisonMode} scope. ${detail.comparison.summary}.`}
          metrics={detail.comparison.metrics}
        />
      ) : (
        <QueryEmptyState
          title="Add a comparison"
          description="Enter realm, area, and resource in the sidebar. All three fields are required to compare this snapshot against another scope."
        />
      )}

      <Card padding="sm" variant="default">
        <CardHeader>
          <Flex justify="between" gap="3" align="start" wrap="wrap">
            <Stack gap="1">
              <CardTitle>Timeline</CardTitle>
              <CardDescription>
                {detail.timeline.derived
                  ? "Derived timeline built from surrounding evidence."
                  : "Live events observed for this resource."}
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
              description="No recent events are visible for this resource. Use the current snapshot or raw payload for exact values."
            />
          ) : (
            <div class="domain-table-wrap">
              <Table>
                <TableHead>
                  <TableRow>
                    <TableHeaderCell>Kind</TableHeaderCell>
                    <TableHeaderCell>Summary</TableHeaderCell>
                    <TableHeaderCell>Observed</TableHeaderCell>
                    <TableHeaderCell>Age</TableHeaderCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  <For
                    each={detail.timeline.events}
                    by={(event) => `${event.observedAt}:${event.summary}`}
                  >
                    {(event) => (
                      <TableRow>
                        <TableCell>{event.kind}</TableCell>
                        <TableCell>{event.summary}</TableCell>
                        <TableCell>{event.observedAt}</TableCell>
                        <TableCell>
                          {event.ageSeconds == null ? "Unknown" : `${event.ageSeconds}s`}
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

      {detail.related.length === 0 ? (
        <QueryEmptyState
          title="No related records"
          description="This resource does not currently expose related tables."
        />
      ) : (
        <For each={detail.related} by={(table) => table.title}>
          {(table) => <RelatedTable table={table} />}
        </For>
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
