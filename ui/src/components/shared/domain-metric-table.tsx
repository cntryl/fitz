import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Section } from "@askrjs/themes/layouts";

export interface DomainMetric {
  label: string;
  value: string | number;
  caption?: string;
}

export interface DomainMetricTableProps {
  title: string;
  metrics: DomainMetric[];
}

function formatValue(value: string | number) {
  return typeof value === "number" ? new Intl.NumberFormat("en-US").format(value) : value;
}

export default function DomainMetricTable({ title, metrics }: DomainMetricTableProps) {
  return (
    <Section class="domain-section" size="3">
      <div class="domain-section-header">
        <div>
          <p class="eyebrow">{title}</p>
          <h2>{metrics.length} metrics</h2>
        </div>
      </div>

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
                  <TableCell>{formatValue(metric.value)}</TableCell>
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
