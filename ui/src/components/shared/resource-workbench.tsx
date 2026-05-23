import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Flex, Section, Stack } from "@askrjs/themes/layouts";
import DomainMetricTable from "./domain-metric-table";
import type { ResourceDetail, ResourceRelatedTable } from "@/features/resource/resource-models";

export interface ResourceWorkbenchProps {
  detail: ResourceDetail;
}

function RelatedTable({ table }: { table: ResourceRelatedTable }) {
  return (
    <Section size="3">
      <div class="domain-section-header">
        <h2>{table.title}</h2>
        <span>{table.rows.length} rows</span>
      </div>
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
    </Section>
  );
}

export default function ResourceWorkbench({ detail }: ResourceWorkbenchProps) {
  return (
    <div class="resource-workbench">
      <Section size="3">
        <Flex justify="between" gap="3" align="start" wrap="wrap">
          <Stack gap="1">
            <h2>{detail.ref.resource}</h2>
            <p>
              {detail.domain} / {detail.ref.realm} / {detail.ref.area} / {detail.ref.resource}
            </p>
          </Stack>
          <Link href={`/${detail.domain}`}>Back to {detail.domain}</Link>
        </Flex>
      </Section>

      <DomainMetricTable title="Overview" metrics={detail.detailMetrics} />

      <Section size="3">
        <div class="domain-section-header">
          <h2>Timeline</h2>
          <span>{detail.timeline.derived ? "Derived" : "Live"}</span>
        </div>

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
      </Section>

      {detail.comparison ? (
        <DomainMetricTable
          title={`Compare: ${detail.comparison.summary}`}
          metrics={detail.comparison.metrics}
        />
      ) : null}

      <For each={detail.related} by={(table) => table.title}>
        {(table) => <RelatedTable table={table} />}
      </For>

      <Section size="3">
        <div class="domain-section-header">
          <h2>Raw API payload</h2>
        </div>
        <pre class="resource-raw">{JSON.stringify(detail.raw, null, 2)}</pre>
      </Section>
    </div>
  );
}
