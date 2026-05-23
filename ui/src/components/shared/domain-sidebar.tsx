import { For } from "@askrjs/askr/control";
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
      <div class="domain-section-header">
        <div>
          <h2>{title}</h2>
          <p>{description}</p>
        </div>
      </div>
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
      {footer ? <div class="domain-sidebar-footer">{footer}</div> : null}
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
