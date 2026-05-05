import { For } from "@askrjs/askr";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/components";

export interface DomainSidebarStat {
  label: string;
  value: string | number;
  note?: string;
}

export interface DomainSidebarProps {
  title: string;
  description: string;
  stats: DomainSidebarStat[];
  footer?: unknown;
}

function formatValue(value: string | number) {
  return typeof value === "number" ? new Intl.NumberFormat("en-US").format(value) : value;
}

export default function DomainSidebar({ title, description, stats, footer }: DomainSidebarProps) {
  return (
    <Card class="domain-sidebar" variant="raised">
      <CardHeader class="domain-sidebar-header">
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent class="domain-sidebar-content">
        <dl class="domain-sidebar-stats">
          <For each={stats} by={(stat) => stat.label}>
            {(stat) => (
              <div class="domain-sidebar-stat">
                <dt>{stat.label}</dt>
                <dd>{formatValue(stat.value)}</dd>
                {stat.note ? <p>{stat.note}</p> : null}
              </div>
            )}
          </For>
        </dl>
      </CardContent>
      {footer ? <CardFooter class="domain-sidebar-footer">{footer}</CardFooter> : null}
    </Card>
  );
}
