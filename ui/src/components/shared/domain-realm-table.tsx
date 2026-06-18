import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
import { QueryEmptyState } from "./query-state";

export interface DomainRealm {
  realm: string;
  note?: string;
}

export interface DomainRealmTableProps {
  title: string;
  realms: DomainRealm[];
  emptyMessage: string;
}

export default function DomainRealmTable({ title, realms, emptyMessage }: DomainRealmTableProps) {
  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <p class="domain-muted">{realms.length} realms</p>
      </CardHeader>

      <CardContent>
        {realms.length === 0 ? (
          <QueryEmptyState description={emptyMessage} />
        ) : (
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
                      <TableCell>{realm.realm}</TableCell>
                      <TableCell>{realm.note ?? "Active"}</TableCell>
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
