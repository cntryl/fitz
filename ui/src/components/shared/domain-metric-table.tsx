import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Section } from "@askrjs/themes/layouts";
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
      <div class="domain-section-header">
        <h2>{title}</h2>
      </div>

      <div class="domain-table-wrap">
        <Table class="domain-table">
          <TableHead>
            <TableRow>
              <TableHeaderCell>Metric</TableHeaderCell>
              <TableHeaderCell>Value</TableHeaderCell>
            </TableRow>
          </TableHead>
          <TableBody>
            <For each={metrics} by={(metric) => metric.label}>
              {(metric) => (
                <TableRow>
                  <TableCell>{metric.label}</TableCell>
                  <TableCell>{formatDisplayValue(metric.value)}</TableCell>
                </TableRow>
              )}
            </For>
          </TableBody>
        </Table>
      </div>
    </Section>
  );
}
