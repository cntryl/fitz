import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
import { QueryEmptyState } from "./query-state";
import type { DomainId, ResourceInventory } from "@/features/resource/resource-models";
import type { QueueInventory } from "@/features/queue/queue-models";

export interface DomainResourceBrowserProps {
  domain: DomainId | "queue";
  inventory?: (ResourceInventory | QueueInventory) | null;
  loading?: boolean;
}

function resourceHref(
  domain: DomainResourceBrowserProps["domain"],
  realm: string,
  area: string,
  resource: string,
) {
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
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>Resources</CardTitle>
        <p class="domain-muted">{loading ? "Loading" : `${rows.length} visible`}</p>
      </CardHeader>

      <CardContent>
        {!loading && rows.length === 0 ? (
          <QueryEmptyState description="No live resources are currently visible for this domain." />
        ) : (
          <div class="domain-table-wrap">
            <Table>
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
                      <TableCell>
                        <span class="domain-table-cell-truncate">{row.realm}</span>
                      </TableCell>
                      <TableCell>
                        <span class="domain-table-cell-truncate">{row.area}</span>
                      </TableCell>
                      <TableCell>
                        <Link
                          class="domain-link-cell"
                          href={resourceHref(domain, row.realm, row.area, row.resource)}
                        >
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
      </CardContent>
    </Card>
  );
}
