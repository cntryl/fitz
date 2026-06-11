import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
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
    <Card class="domain-metric-card" padding="sm" variant="default">
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
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
                    <TableCell>
                      <span class="domain-metric-value">{formatDisplayValue(metric.value)}</span>
                      {metric.caption ? (
                        <span class="domain-metric-caption">{metric.caption}</span>
                      ) : null}
                    </TableCell>
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
