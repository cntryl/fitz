import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Section, Stack } from "@askrjs/themes/layouts";
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
    <Section size="3">
      <Stack gap="1">
        <p class="eyebrow">{title}</p>
        <h2>{realms.length} realms</h2>
      </Stack>

      {realms.length === 0 ? (
        <QueryEmptyState description={emptyMessage} />
      ) : (
        <div class="domain-table-wrap">
          <Table class="domain-table">
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
    </Section>
  );
}
