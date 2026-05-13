import { For } from "@askrjs/askr";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { GaugeIcon } from "@askrjs/lucide";
import { EmptyState, Section } from "@askrjs/themes/components";

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
    <Section class="domain-section" size="3">
      <div class="domain-section-header">
        <div>
          <p class="eyebrow">{title}</p>
          <h2>{realms.length} realms</h2>
        </div>
      </div>

      {realms.length === 0 ? (
        <EmptyState
          class="domain-state"
          icon={<GaugeIcon size={18} />}
          description={emptyMessage}
        />
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
