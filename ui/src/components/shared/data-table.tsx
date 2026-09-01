import { For, Show } from "@askrjs/askr/control";
import type { JSXElement } from "@askrjs/askr/foundations/structures";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";

export interface DataTableCellProps<Row> {
  column: DataTableColumn<Row>;
  row: Row;
  rowIndex: number;
  rowKey: string;
  selected: false;
}

export interface DataTableColumn<Row> {
  cellComponent: (props: DataTableCellProps<Row>) => JSXElement | JSX.Element | null;
  header: JSXElement | string;
  id: string;
  width?: number | string;
}

export interface DataTableProps<Row> {
  ariaLabel: string;
  class?: string;
  columns: readonly DataTableColumn<Row>[];
  dataHasMetrics?: boolean;
  getKey: (row: Row, index: number) => string | number;
  id?: string;
  onRowClick?: (row: Row, rowIndex: number, rowKey: string, event: MouseEvent) => void;
  rows: readonly Row[];
}

export default function DataTable<Row>({
  ariaLabel,
  class: className,
  columns,
  dataHasMetrics,
  getKey,
  id,
  onRowClick,
  rows,
}: DataTableProps<Row>) {
  const visibleRows = rows.slice(0, 500);

  return (
    <div
      class={`domain-table-wrap${className ? ` ${className}` : ""}`}
      data-has-metrics={dataHasMetrics ? "true" : undefined}
    >
      <Table id={id} aria-label={ariaLabel}>
        <colgroup>
          <For each={columns} by={(column) => column.id}>
            {(column) => (
              <col
                style={{
                  width: typeof column.width === "number" ? `${column.width}px` : column.width,
                }}
              />
            )}
          </For>
        </colgroup>
        <TableHead>
          <TableRow>
            <For each={columns} by={(column) => column.id}>
              {(column) => (
                <TableHeaderCell data-column-id={column.id}>{column.header}</TableHeaderCell>
              )}
            </For>
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={visibleRows as Row[]} by={(row, index) => String(getKey(row, index))}>
            {(row, rowIndex) => {
              const index = rowIndex();
              const rowKey = String(getKey(row, index));

              return (
                <TableRow
                  data-row-index={String(index)}
                  data-row-key={rowKey}
                  onClick={(event: MouseEvent) => onRowClick?.(row, index, rowKey, event)}
                >
                  <For each={columns} by={(column) => column.id}>
                    {(column) => (
                      <TableCell data-column-id={column.id}>
                        {column.cellComponent({
                          column,
                          row,
                          rowIndex: index,
                          rowKey,
                          selected: false,
                        })}
                      </TableCell>
                    )}
                  </For>
                </TableRow>
              );
            }}
          </For>
        </TableBody>
      </Table>
      <Show when={rows.length > visibleRows.length}>
        <p class="domain-table-limit-notice" role="status">
          Showing the first {visibleRows.length} of {rows.length} rows. Refine the current scope or
          filter to inspect later rows.
        </p>
      </Show>
    </div>
  );
}
