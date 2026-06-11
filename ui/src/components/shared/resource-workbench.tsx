import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Flex, Stack } from "@askrjs/themes/layouts";
import { Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
import DomainMetricTable from "./domain-metric-table";
import type { ResourceDetail, ResourceRelatedTable } from "@/features/resource/resource-models";

export interface ResourceWorkbenchProps {
  detail: ResourceDetail;
}

function RelatedTable({ table }: { table: ResourceRelatedTable }) {
  return (
    <Card class="resource-related-card" padding="sm" variant="default">
      <CardHeader>
        <CardTitle>{table.title}</CardTitle>
        <p class="domain-muted">{table.rows.length} rows</p>
      </CardHeader>
      <CardContent>
        <div class="domain-table-wrap">
          <Table class="domain-table">
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
      </CardContent>
    </Card>
  );
}

export default function ResourceWorkbench({ detail }: ResourceWorkbenchProps) {
  return (
    <Stack gap="4" class="resource-workbench">
      <section class="resource-workbench-hero">
        <Flex justify="between" gap="3" align="start" wrap="wrap">
          <Stack gap="1" class="resource-workbench-title">
            <p class="domain-header-kicker">{detail.domain}</p>
            <h2>{detail.ref.resource}</h2>
            <p>
              {detail.ref.realm} / {detail.ref.area} / {detail.ref.resource}
            </p>
          </Stack>
          <Link href={`/${detail.domain}`}>Back to {detail.domain}</Link>
        </Flex>
      </section>

      <DomainMetricTable title="Overview" metrics={detail.detailMetrics} />

      <Card class="resource-workbench-timeline" padding="sm" variant="default">
        <CardHeader>
          <CardTitle>Timeline</CardTitle>
          <p class="domain-muted">{detail.timeline.derived ? "Derived" : "Live"}</p>
        </CardHeader>
        <CardContent>
          {detail.timeline.events.length === 0 ? (
            <p class="domain-muted">No recent events are visible for this resource.</p>
          ) : (
            <div class="domain-table-wrap">
              <Table class="domain-table">
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

      {detail.comparison ? (
        <DomainMetricTable
          title={`Compare: ${detail.comparison.summary}`}
          metrics={detail.comparison.metrics}
        />
      ) : null}

      <For each={detail.related} by={(table) => table.title}>
        {(table) => <RelatedTable table={table} />}
      </For>

      <section class="resource-workbench-raw">
        <Card class="resource-workbench-raw-card" padding="sm" variant="default">
          <CardHeader>
            <CardTitle>Raw API payload</CardTitle>
          </CardHeader>
          <CardContent>
            <pre class="resource-raw">{JSON.stringify(detail.raw, null, 2)}</pre>
          </CardContent>
        </Card>
      </section>
    </Stack>
  );
}
