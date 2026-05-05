import { For } from "@askrjs/askr";
import { Link } from "@askrjs/askr/router";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Section,
} from "@askrjs/themes/components";
import type { DomainLink } from "@/shared/navigation/domains";

export interface DomainIndexProps {
  title: string;
  description: string;
  links: DomainLink[];
}

export default function DomainIndex({ title, description, links }: DomainIndexProps) {
  return (
    <Section class="domain-index" size="3">
      <div class="domain-header-copy">
        <p class="eyebrow">Navigation</p>
        <h2>{title}</h2>
        <p>{description}</p>
      </div>

      <div class="domain-index-grid">
        <For each={links} by={(link) => link.href}>
          {(link) => (
            <Link href={link.href} class="domain-index-link">
              <Card class="domain-index-card">
                <CardHeader class="domain-index-card-header">
                  <span class="domain-index-icon">
                    <link.icon size={18} />
                  </span>
                  <CardTitle>{link.title}</CardTitle>
                </CardHeader>
                <CardContent>
                  <CardDescription>{link.description}</CardDescription>
                </CardContent>
              </Card>
            </Link>
          )}
        </For>
      </div>
    </Section>
  );
}
