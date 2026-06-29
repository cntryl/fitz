import { Link, navigate } from "@askrjs/askr/router";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import { Text } from "@askrjs/themes/components";
import { QueryEmptyState } from "./query-state";
import type { ResourceInventoryResource } from "@/features/resource/resource-models";
import { formatNumber } from "@/shared/format";
import { domainResourceHref, type DomainSegment } from "@/shared/navigation/domains";

export interface DomainResourceInventoryArea {
  area: string;
  resourceEntries?: ResourceInventoryResource[];
  resources: string[];
}

export interface DomainResourceInventoryRealm {
  areas: DomainResourceInventoryArea[];
  realm: string;
}

export interface DomainResourceInventory {
  realms: DomainResourceInventoryRealm[];
}

export interface DomainResourceInventoryRow extends ResourceInventoryResource {
  area: string;
  realm: string;
}

export interface DomainResourceMetricColumn {
  cell: (row: DomainResourceInventoryRow) => unknown;
  header: string;
  id: string;
  title?: (row: DomainResourceInventoryRow) => string | undefined;
  width?: string;
}

export interface DomainResourceInventoryTableProps {
  domain: DomainSegment;
  emptyDescription: string;
  inventory?: DomainResourceInventory | null;
  metricColumns?: readonly DomainResourceMetricColumn[];
  title: string;
}

export function domainResourceInventoryRows(
  inventory: DomainResourceInventory | null | undefined,
): DomainResourceInventoryRow[] {
  return (
    inventory?.realms.flatMap((realm) =>
      realm.areas.flatMap((area) => {
        const entries = area.resourceEntries;
        const resourceEntries =
          entries && entries.length > 0
            ? entries
            : area.resources.map((resource) => ({ resource }));

        return resourceEntries.map((resource) => ({
          area: area.area,
          realm: realm.realm,
          ...resource,
        }));
      }),
    ) ?? []
  );
}

export function DomainResourceMetricText(props: { children: unknown; title?: string }) {
  return (
    <Text
      as="span"
      class="domain-resource-metric"
      font="mono"
      numeric="tabular"
      size="sm"
      title={props.title}
      weight="medium"
    >
      {props.children}
    </Text>
  );
}

function tableHeight(rowCount: number) {
  return `${Math.min(620, Math.max(280, 44 + rowCount * 48))}px`;
}

function shouldIgnoreRowClick(event: MouseEvent) {
  if (event.defaultPrevented) return true;
  const target = event.target;
  return target instanceof Element && target.closest("a,button,input,select,textarea") !== null;
}

function openResource(domain: DomainSegment, row: DomainResourceInventoryRow) {
  navigate(domainResourceHref(domain, row));
}

export default function DomainResourceInventoryTable({
  domain,
  emptyDescription,
  inventory,
  metricColumns = [],
  title,
}: DomainResourceInventoryTableProps) {
  const rows = domainResourceInventoryRows(inventory);
  const hasMetrics = metricColumns.length > 0;
  const columns: readonly VirtualTableColumn<DomainResourceInventoryRow>[] = [
    {
      id: "realm",
      header: "Realm",
      width: hasMetrics ? "15%" : "28%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.realm}>
          {row.realm}
        </span>
      ),
    },
    {
      id: "area",
      header: "Area",
      width: hasMetrics ? "15%" : "28%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.area}>
          {row.area}
        </span>
      ),
    },
    {
      id: "resource",
      header: "Resource",
      width: hasMetrics ? "20%" : "44%",
      cellComponent: ({ row }) => (
        <Link class="domain-link-cell" href={domainResourceHref(domain, row)}>
          {row.resource}
        </Link>
      ),
    },
    ...metricColumns.map(
      (column): VirtualTableColumn<DomainResourceInventoryRow> => ({
        id: column.id,
        header: column.header,
        width: column.width,
        cellComponent: ({ row }) => (
          <DomainResourceMetricText title={column.title?.(row)}>
            {column.cell(row)}
          </DomainResourceMetricText>
        ),
      }),
    ),
  ];

  return (
    <section
      class="domain-section domain-resource-inventory"
      aria-labelledby={`${domain}-inventory`}
    >
      <div class="domain-section-header">
        <div>
          <h2 id={`${domain}-inventory`}>{title}</h2>
        </div>
        <span>{formatNumber(rows.length)} visible</span>
      </div>

      {rows.length === 0 ? (
        <QueryEmptyState description={emptyDescription} />
      ) : (
        <VirtualTable<DomainResourceInventoryRow>
          aria-label={title}
          class="domain-resource-virtual-table"
          columns={columns}
          getKey={(row) => `${row.realm}:${row.area}:${row.resource}`}
          headerHeight={44}
          onRowClick={(row, _rowIndex, _rowKey, event) => {
            if (!shouldIgnoreRowClick(event)) {
              openResource(domain, row);
            }
          }}
          overscan={8}
          rowHeight={48}
          rows={rows}
          style={{ height: tableHeight(rows.length) }}
        />
      )}
    </section>
  );
}
