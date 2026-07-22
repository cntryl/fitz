import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  type CardTitleHeadingTag,
  Text,
} from "@askrjs/themes/components";
import { formatDisplayValue } from "@/shared/format";

export interface DomainMetric {
  label: string;
  value: string | number;
  caption?: string;
}

export interface DomainMetricTableProps {
  description?: string;
  title: string;
  titleAs?: CardTitleHeadingTag;
  metrics: DomainMetric[];
}

export default function DomainMetricTable({
  description,
  metrics,
  title,
  titleAs = "h2",
}: DomainMetricTableProps) {
  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle titleAs={titleAs}>{title}</CardTitle>
        {description ? <CardDescription>{description}</CardDescription> : null}
      </CardHeader>
      <CardContent>
        <div class="domain-table-wrap domain-metric-table-wrap">
          <Table>
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
                      <Text
                        as="span"
                        class="domain-metric-value"
                        font="mono"
                        numeric="tabular"
                        weight="semibold"
                      >
                        {formatDisplayValue(metric.value)}
                      </Text>
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
