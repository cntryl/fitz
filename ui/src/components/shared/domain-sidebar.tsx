import { For } from "@askrjs/askr/control";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import { Stack } from "@askrjs/themes/layouts";
import { formatDisplayValue } from "@/shared/format";

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

export interface DomainSidebarConfig<TData> {
  data: TData | null | undefined;
  title: string;
  description: string;
  stats: (data: TData) => DomainSidebarStat[];
  footer?: unknown;
}

export default function DomainSidebar({ title, description, stats, footer }: DomainSidebarProps) {
  return (
    <aside class="domain-sidebar">
      <Card class="domain-sidebar-card" padding="sm" variant="default">
        <CardHeader>
          <CardTitle>{title}</CardTitle>
          <CardDescription>{description}</CardDescription>
        </CardHeader>

        <CardContent>
          <dl class="domain-sidebar-stats">
            <For each={stats} by={(stat) => stat.label}>
              {(stat) => (
                <div class="domain-sidebar-stat">
                  <dt>{stat.label}</dt>
                  <dd>{formatDisplayValue(stat.value)}</dd>
                  {stat.note ? <p>{stat.note}</p> : null}
                </div>
              )}
            </For>
          </dl>
        </CardContent>

        {footer ? (
          <CardFooter>
            <Stack gap="3">{footer}</Stack>
          </CardFooter>
        ) : null}
      </Card>
    </aside>
  );
}

export function createDomainSidebar<TData>({
  data,
  title,
  description,
  stats,
  footer,
}: DomainSidebarConfig<TData>) {
  if (!data) {
    return undefined;
  }

  return (
    <DomainSidebar title={title} description={description} stats={stats(data)} footer={footer} />
  );
}
