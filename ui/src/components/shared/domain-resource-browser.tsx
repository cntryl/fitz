import { Link } from "@askrjs/askr/router";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import { Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/components";
import { QueryEmptyState, QueryErrorState } from "./query-state";
import type { QueueInventory } from "@/features/queue/queue-models";
import type { ResourceInventory } from "@/features/resource/resource-models";
import { domainResourceHref, type DomainSegment } from "@/shared/navigation/domains";

export interface DomainResourceBrowserProps {
  domain: DomainSegment;
  error?: unknown;
  inventory?: (ResourceInventory | QueueInventory) | null;
  loading?: boolean;
}

interface ResourceBrowserRow {
  area: string;
  realm: string;
  resource: string;
}

export default function DomainResourceBrowser({
  domain,
  error,
  inventory,
  loading = false,
}: DomainResourceBrowserProps) {
  const rows: ResourceBrowserRow[] =
    inventory?.realms.flatMap((realm) =>
      realm.areas.flatMap((area) =>
        area.resources.map((resource) => ({
          area: area.area,
          realm: realm.realm,
          resource,
        })),
      ),
    ) ?? [];
  const columns: readonly VirtualTableColumn<ResourceBrowserRow>[] = [
    {
      id: "realm",
      header: "Realm",
      width: "28%",
      cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.realm}</span>,
    },
    {
      id: "area",
      header: "Area",
      width: "28%",
      cellComponent: ({ row }) => <span class="domain-table-cell-truncate">{row.area}</span>,
    },
    {
      id: "resource",
      header: "Resource",
      width: "44%",
      cellComponent: ({ row }) => (
        <Link
          class="domain-link-cell"
          href={domainResourceHref(domain, {
            area: row.area,
            realm: row.realm,
            resource: row.resource,
          })}
        >
          {row.resource}
        </Link>
      ),
    },
  ];

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>Resources</CardTitle>
        <p class="domain-muted">{loading ? "Loading" : `${rows.length} visible`}</p>
      </CardHeader>

      <CardContent>
        {error ? (
          <QueryErrorState title="Unable to load resources" error={error} />
        ) : !loading && rows.length === 0 ? (
          <QueryEmptyState description="No live resources are currently visible for this domain." />
        ) : (
          <VirtualTable<ResourceBrowserRow>
            aria-label={`${domain} resources`}
            class="domain-resource-virtual-table"
            columns={columns}
            getKey={(row) => `${row.realm}:${row.area}:${row.resource}`}
            headerHeight={44}
            overscan={6}
            rowHeight={48}
            rows={rows}
            style={{ height: "320px" }}
          />
        )}
      </CardContent>
    </Card>
  );
}
