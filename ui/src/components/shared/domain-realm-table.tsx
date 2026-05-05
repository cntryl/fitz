import { For } from "@askrjs/askr";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import DomainState from "@/components/shared/domain-state";

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
    <section class="domain-section">
      <div class="domain-section-header">
        <div>
          <p class="eyebrow">{title}</p>
          <h2>{realms.length} realms</h2>
        </div>
      </div>

      {realms.length === 0 ? (
        <DomainState kind="empty" message={emptyMessage} />
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
    </section>
  );
}
