import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Section, Stack } from "@askrjs/themes/layouts";
import { formatDisplayValue } from "@/shared/format";

export interface DomainMetric {
  label: string;
  value: string | number;
  caption?: string;
}

export interface DomainMetricTableProps {
  title: string;
  metrics: DomainMetric[];
}

export default function DomainMetricTable({ title, metrics }: DomainMetricTableProps) {
  return (
    <Section size="3">
      <Stack gap="1">
        <p class="eyebrow">{title}</p>
        <h2>{metrics.length} metrics</h2>
      </Stack>

      <div class="domain-table-wrap">
        <Table class="domain-table">
          <TableHead>
            <TableRow>
              <TableHeaderCell>Metric</TableHeaderCell>
              <TableHeaderCell>Value</TableHeaderCell>
              <TableHeaderCell>Notes</TableHeaderCell>
            </TableRow>
          </TableHead>
          <TableBody>
            <For each={metrics} by={(metric) => metric.label}>
              {(metric) => (
                <TableRow>
                  <TableCell>{metric.label}</TableCell>
                  <TableCell>{formatDisplayValue(metric.value)}</TableCell>
                  <TableCell>{metric.caption ?? "Live broker snapshot"}</TableCell>
                </TableRow>
              )}
            </For>
          </TableBody>
        </Table>
      </div>
    </Section>
  );
}
