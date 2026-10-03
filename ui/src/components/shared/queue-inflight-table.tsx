import DataTable, { type DataTableColumn } from "./data-table";
import TitledCell from "./titled-cell";
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
      width: "76%",
      cellComponent: ({ row }) => (
        <TitledCell
          subtitle={`Expires ${formatTimestamp(row.expiresAt)} · session ${row.sessionId} · token ${row.inflightToken}`}
        >
          {row.messageId}
        </TitledCell>
      ),
    },
    {
      id: "attempts",
      header: "Attempts",
      width: "24%",
      cellComponent: ({ row }) => <span>{row.attempts}</span>,
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
