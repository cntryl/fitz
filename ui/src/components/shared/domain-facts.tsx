import { For } from "@askrjs/askr/control";

export interface DomainFact {
  label: string;
  title?: string;
  value: number | string;
}

export interface DomainFactsProps {
  id: string;
  items: readonly DomainFact[];
  title: string;
}

export default function DomainFacts({ id, items, title }: DomainFactsProps) {
  return (
    <section class="domain-facts-section" aria-labelledby={id}>
      <h2 id={id}>{title}</h2>
      <dl class="domain-facts">
        <For each={items as DomainFact[]} by={(item) => item.label}>
          {(item) => (
            <div class="domain-fact">
              <dt>{item.label}</dt>
              <dd title={item.title}>{item.value}</dd>
            </div>
          )}
        </For>
      </dl>
    </section>
  );
}
