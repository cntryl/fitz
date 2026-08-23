import { For, Show } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import type {
  DomainResourceInventoryArea,
  DomainResourceInventoryRealm,
} from "./domain-resource-inventory-table";
import { QueryCompactEmptyState } from "./query-state";
import { formatNumber } from "@/shared/format";
import { domainScopeHref, type DomainSegment } from "@/shared/navigation/domains";

interface DomainScopeInventoryTableProps {
  areas?: readonly DomainResourceInventoryArea[];
  domain: DomainSegment;
  emptyDescription: string;
  realms?: readonly DomainResourceInventoryRealm[];
  realm?: string;
}

function resourceCount(area: DomainResourceInventoryArea) {
  return area.resourceEntries?.length || area.resources.length;
}

export default function DomainScopeInventoryTable({
  areas = [],
  domain,
  emptyDescription,
  realms = [],
  realm,
}: DomainScopeInventoryTableProps) {
  const showingAreas = realm !== undefined;
  const rows = showingAreas ? areas : realms;
  const title = showingAreas ? "Areas" : "Realms";

  return (
    <section class="domain-section domain-scope-inventory" aria-labelledby={`${domain}-inventory`}>
      <div class="domain-section-header">
        <div>
          <h2 id={`${domain}-inventory`}>{title}</h2>
          <p>Select {showingAreas ? "an area" : "a realm"} to continue the drilldown.</p>
        </div>
        <span role="status">{formatNumber(rows.length)} visible</span>
      </div>

      <Show
        when={rows.length > 0}
        fallback={<QueryCompactEmptyState description={emptyDescription} />}
      >
        <div class="domain-table-wrap">
          <Table aria-label={title}>
            <TableHead>
              <TableRow>
                <TableHeaderCell>{showingAreas ? "Area" : "Realm"}</TableHeaderCell>
                {showingAreas ? null : <TableHeaderCell>Areas</TableHeaderCell>}
                <TableHeaderCell>Resources</TableHeaderCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {showingAreas ? (
                <For each={areas as DomainResourceInventoryArea[]} by={(area) => area.area}>
                  {(area) => {
                    const href = domainScopeHref(domain, { realm, area: area.area });

                    return (
                      <TableRow>
                        <TableCell>
                          <a class="domain-link-cell" href={href}>
                            {area.area}
                          </a>
                        </TableCell>
                        <TableCell>{formatNumber(resourceCount(area))}</TableCell>
                      </TableRow>
                    );
                  }}
                </For>
              ) : (
                <For each={realms as DomainResourceInventoryRealm[]} by={(item) => item.realm}>
                  {(item) => {
                    const href = domainScopeHref(domain, { realm: item.realm });

                    return (
                      <TableRow>
                        <TableCell>
                          <a class="domain-link-cell" href={href}>
                            {item.realm}
                          </a>
                        </TableCell>
                        <TableCell>{formatNumber(item.areas.length)}</TableCell>
                        <TableCell>
                          {formatNumber(
                            item.areas.reduce((total, area) => total + resourceCount(area), 0),
                          )}
                        </TableCell>
                      </TableRow>
                    );
                  }}
                </For>
              )}
            </TableBody>
          </Table>
        </div>
      </Show>
    </section>
  );
}
