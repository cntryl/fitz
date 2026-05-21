import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Section } from "@askrjs/themes/layouts";
import { QueryEmptyState } from "./query-state";
import type { DomainId, ResourceInventory } from "@/features/resource/resource-models";

export interface DomainResourceBrowserProps {
  domain: DomainId | "queue";
  inventory?: ResourceInventory | null;
  loading?: boolean;
}

function resourceHref(domain: DomainResourceBrowserProps["domain"], realm: string, area: string, resource: string) {
  return `/${domain}/${encodeURIComponent(realm)}/${encodeURIComponent(area)}/${encodeURIComponent(resource)}`;
}

export default function DomainResourceBrowser({
  domain,
  inventory,
  loading = false,
}: DomainResourceBrowserProps) {
  const rows =
    inventory?.realms.flatMap((realm) =>
      realm.areas.flatMap((area) =>
        area.resources.map((resource) => ({
          area: area.area,
          realm: realm.realm,
          resource,
        })),
      ),
    ) ?? [];

  return (
    <Section class="domain-section" size="3">
      <div class="domain-section-header">
        <div>
          <p class="eyebrow">Resource browser</p>
          <h2>{loading ? "Loading resources" : `${rows.length} resources`}</h2>
        </div>
      </div>

      {!loading && rows.length === 0 ? (
        <QueryEmptyState description="No warm resources are currently visible for this domain." />
      ) : (
        <div class="domain-table-wrap">
          <Table class="domain-table">
            <TableHead>
              <TableRow>
                <TableHeaderCell>Realm</TableHeaderCell>
                <TableHeaderCell>Area</TableHeaderCell>
                <TableHeaderCell>Resource</TableHeaderCell>
              </TableRow>
            </TableHead>
            <TableBody>
              <For each={rows} by={(row) => `${row.realm}:${row.area}:${row.resource}`}>
                {(row) => (
                  <TableRow>
                    <TableCell>{row.realm}</TableCell>
                    <TableCell>{row.area}</TableCell>
                    <TableCell>
                      <Link href={resourceHref(domain, row.realm, row.area, row.resource)}>
                        {row.resource}
                      </Link>
                    </TableCell>
                  </TableRow>
                )}
              </For>
            </TableBody>
          </Table>
        </div>
      )}
    </Section>
  );
}
