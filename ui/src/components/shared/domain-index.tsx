import { For } from "@askrjs/askr";
import { Link } from "@askrjs/askr/router";
import type { DomainLink } from "@/shared/navigation/domains";

export interface DomainIndexProps {
  title: string;
  description: string;
  links: DomainLink[];
}

export default function DomainIndex({ title, description, links }: DomainIndexProps) {
  return (
    <section class="domain-index">
      <div class="domain-header-copy">
        <p class="eyebrow">Navigation</p>
        <h2>{title}</h2>
        <p>{description}</p>
      </div>

      <div class="domain-index-grid">
        <For each={links} by={(link) => link.href}>
          {(link) => (
            <Link href={link.href} class="domain-index-card">
              <strong>{link.title}</strong>
              <span>{link.description}</span>
            </Link>
          )}
        </For>
      </div>
    </section>
  );
}
