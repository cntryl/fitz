import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Badge, Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
import { Flex, Section, Stack } from "@askrjs/themes/layouts";
import DomainMetricTable from "./domain-metric-table";
import type {
  ResourceDetail,
  ResourceMetric,
  ResourceRelatedTable,
} from "@/features/resource/resource-models";
import { formatDisplayValue } from "@/shared/format";

export interface ResourceWorkbenchProps {
  detail: ResourceDetail;
}

function RelatedTable({ table }: { table: ResourceRelatedTable }) {
  return (
    <Section size="3">
      <Stack gap="1">
        <p class="eyebrow">{table.title}</p>
        <h2>{table.rows.length} rows</h2>
      </Stack>
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

function MetricStrip({ metrics }: { metrics: ResourceMetric[] }) {
  return (
    <div class="resource-metric-strip">
      <For each={metrics} by={(metric) => metric.label}>
        {(metric) => (
          <div class="resource-metric">
            <span>{metric.label}</span>
            <strong>{formatDisplayValue(metric.value)}</strong>
          </div>
        )}
      </For>
    </div>
  );
}

export default function ResourceWorkbench({ detail }: ResourceWorkbenchProps) {
  return (
    <div class="resource-workbench">
      <Section size="3">
        <Flex justify="between" gap="3" align="start" wrap="wrap">
          <Stack gap="1">
            <p class="eyebrow">{detail.domain} resource</p>
            <h2>{detail.ref.resource}</h2>
            <p>
              {detail.ref.realm} / {detail.ref.area} / {detail.ref.resource}
            </p>
          </Stack>
          <Link href={`/${detail.domain}`}>Back to {detail.domain}</Link>
        </Flex>
        <MetricStrip metrics={detail.detailMetrics} />
      </Section>

      <DomainMetricTable title="Overview" metrics={detail.detailMetrics} />

      <Section size="3">
        <Flex justify="between" gap="3" align="start" wrap="wrap">
          <Stack gap="1">
            <p class="eyebrow">Timeline</p>
            <h2>{detail.timeline.events.length} events</h2>
          </Stack>
          <Badge>{detail.timeline.derived ? "Derived" : "Live"}</Badge>
        </Flex>
        <Stack gap="3">
          <For each={detail.timeline.events} by={(event) => `${event.observedAt}:${event.summary}`}>
            {(event) => (
              <Card class="domain-resource-card" variant="raised">
                <CardHeader>
                  <Badge>{event.kind}</Badge>
                  <CardTitle>{event.summary}</CardTitle>
                </CardHeader>
                <CardContent>
                  <p>{event.observedAt}</p>
                  <p>
                    {event.ageSeconds == null ? "Age unknown" : `${event.ageSeconds}s old`}
                    {event.correlationId ? ` | Correlation ${event.correlationId}` : ""}
                  </p>
                </CardContent>
              </Card>
            )}
          </For>
        </Stack>
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
        <Stack gap="1">
          <p class="eyebrow">Raw</p>
          <h2>API payload</h2>
        </Stack>
        <pre class="resource-raw">{JSON.stringify(detail.raw, null, 2)}</pre>
      </Section>
    </div>
  );
}
