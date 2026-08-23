import { For, Show } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/components";
import { QueryEmptyState } from "./query-state";
import { domainScopeHref, type DomainSegment } from "@/shared/navigation/domains";

export interface DomainRealm {
  realm: string;
  note?: string;
}

export interface DomainRealmTableProps {
  domain?: DomainSegment;
  title: string;
  realms: DomainRealm[];
  emptyMessage: string;
}

export default function DomainRealmTable({
  domain,
  title,
  realms,
  emptyMessage,
}: DomainRealmTableProps) {
  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle titleAs="h2">{title}</CardTitle>
        <p class="domain-muted">{realms.length} realms</p>
      </CardHeader>

      <CardContent>
        <Show when={realms.length === 0}>
          <QueryEmptyState description={emptyMessage} />
        </Show>
        <Show when={realms.length > 0}>
          <div class="domain-table-wrap">
            <Table>
              <TableHead>
                <TableRow>
                  <TableHeaderCell>Realm</TableHeaderCell>
                  <TableHeaderCell>Notes</TableHeaderCell>
                </TableRow>
              </TableHead>
              <TableBody>
                <For each={realms} by={(realm) => realm.realm}>
                  {(realm) => (
                    <TableRow>
                      <TableCell>
                        {domain ? (
                          <Link
                            class="domain-link-cell"
                            href={domainScopeHref(domain, { realm: realm.realm })}
                            title={realm.realm}
                          >
                            {realm.realm}
                          </Link>
                        ) : (
                          <span class="domain-table-cell-truncate" title={realm.realm}>
                            {realm.realm}
                          </span>
                        )}
                      </TableCell>
                      <TableCell>
                        <span class="domain-table-cell-truncate" title={realm.note ?? "Active"}>
                          {realm.note ?? "Active"}
                        </span>
                      </TableCell>
                    </TableRow>
                  )}
                </For>
              </TableBody>
            </Table>
          </div>
        </Show>
      </CardContent>
    </Card>
  );
}
