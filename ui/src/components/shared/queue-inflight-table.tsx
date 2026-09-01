import DataTable, { type DataTableColumn } from "./data-table";
import type { QueueInflightMessage } from "@/features/queue/queue-resource-models";
import { formatTimestamp } from "@/shared/format";

export interface QueueInflightTableProps {
  messages: QueueInflightMessage[];
}

export default function QueueInflightTable({ messages }: QueueInflightTableProps) {
  const columns: readonly DataTableColumn<QueueInflightMessage>[] = [
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
      id: "token",
      header: "Owner token",
      width: "24%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.inflightToken}>
          {row.inflightToken}
        </span>
      ),
    },
    {
      id: "session",
      header: "Session",
      width: "20%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.sessionId}>
          {row.sessionId}
        </span>
      ),
    },
    {
      id: "attempts",
      header: "Attempts",
      width: "12%",
      cellComponent: ({ row }) => <span>{row.attempts}</span>,
    },
    {
      id: "expires",
      header: "Expires",
      width: "32%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={formatTimestamp(row.expiresAt)}>
          {formatTimestamp(row.expiresAt)}
        </span>
      ),
    },
  ];
  return (
    <DataTable<QueueInflightMessage>
      ariaLabel="Inflight queue messages"
      class="queue-resource-data-table"
      columns={columns}
      getKey={(message) => message.messageId}
      rows={messages}
    />
  );
}
