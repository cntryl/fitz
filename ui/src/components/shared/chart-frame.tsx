import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import { Section, Stack } from "@askrjs/themes/layouts";

export interface ChartShellProps {
  children?: unknown;
  className?: string;
  description?: string;
  scope?: string;
  title: string;
}

export interface ChartPanelProps {
  children?: unknown;
  description?: string;
  title: string;
}

export function ChartShell({ children, className, description, scope, title }: ChartShellProps) {
  return (
    <Section class={className ? `chart-shell ${className}` : "chart-shell"} size="3">
      <div class="chart-shell-header">
        <div class="chart-shell-copy">
          <h2>{title}</h2>
          {description ? <p>{description}</p> : null}
        </div>
        {scope ? <Badge variant="outline">{scope}</Badge> : null}
      </div>
      <Stack gap="3">{children}</Stack>
    </Section>
  );
}

export function ChartPanel({ children, description, title }: ChartPanelProps) {
  return (
    <Card class="chart-panel" padding="sm" variant="default">
      <CardHeader class="chart-panel-header">
        <CardTitle>{title}</CardTitle>
        {description ? <CardDescription>{description}</CardDescription> : null}
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  );
}
