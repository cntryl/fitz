import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import type { QueueInflightMessage } from "@/features/queue/queue-resource-models";
import { formatTimestamp } from "@/shared/format";

export interface QueueInflightTableProps {
  messages: QueueInflightMessage[];
}

export default function QueueInflightTable({ messages }: QueueInflightTableProps) {
  const columns: readonly VirtualTableColumn<QueueInflightMessage>[] = [
    {
      id: "message",
      header: "Message",
      width: "12%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={String(row.messageId)}>
          {row.messageId}
        </span>
      ),
    },
    {
      id: "context",
      header: "Context",
      width: "22%",
      cellComponent: ({ row }) => {
        const context = `${row.realm} / ${row.area} / ${row.resource}`;

        return (
          <span class="domain-table-cell-truncate" title={context}>
            {context}
          </span>
        );
      },
    },
    {
      id: "token",
      header: "Owner token",
      width: "17%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.inflightToken}>
          {row.inflightToken}
        </span>
      ),
    },
    {
      id: "session",
      header: "Session",
      width: "15%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.sessionId}>
          {row.sessionId}
        </span>
      ),
    },
    {
      id: "family",
      header: "Family",
      width: "7%",
      cellComponent: ({ row }) => <span>{row.family}</span>,
    },
    {
      id: "attempts",
      header: "Attempts",
      width: "7%",
      cellComponent: ({ row }) => <span>{row.attempts}</span>,
    },
    {
      id: "expires",
      header: "Expires",
      width: "20%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={formatTimestamp(row.expiresAt)}>
          {formatTimestamp(row.expiresAt)}
        </span>
      ),
    },
  ];
  const tableHeight = Math.min(420, Math.max(144, 44 + messages.length * 48));

  return (
    <VirtualTable<QueueInflightMessage>
      aria-label="Inflight queue messages"
      class="queue-resource-virtual-table"
      columns={columns}
      getKey={(message) => message.messageId}
      headerHeight={44}
      overscan={8}
      rowHeight={48}
      rows={messages}
      style={{ height: `${tableHeight}px` }}
    />
  );
}
