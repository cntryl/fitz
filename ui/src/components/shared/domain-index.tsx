import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
import type { DomainLink } from "@/shared/navigation/domains";

export interface DomainIndexProps {
  title: string;
  description: string;
  links: DomainLink[];
}

export default function DomainIndex({ title, description, links }: DomainIndexProps) {
  return (
    <Card class="domain-index-card" padding="sm" variant="default">
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent>
        <nav aria-label={title}>
          <ul class="domain-link-list">
            <For each={links} by={(link) => link.href}>
              {(link) => (
                <li>
                  <Link href={link.href} class="domain-link-row">
                    <span class="domain-link-title">
                      <link.icon size={16} />
                      {link.title}
                    </span>
                    <span>{link.description}</span>
                  </Link>
                </li>
              )}
            </For>
          </ul>
        </nav>
      </CardContent>
    </Card>
  );
}
