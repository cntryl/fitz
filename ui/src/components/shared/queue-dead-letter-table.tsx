import DataTable, { type DataTableColumn } from "./data-table";
import TitledCell from "./titled-cell";
import { RefreshCwIcon, Trash2Icon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/components";
import type { DeadLetterMessage } from "@/features/queue/queue-models";
import { formatTimestamp } from "@/shared/format";

export interface QueueDeadLetterTableProps {
  messages: DeadLetterMessage[];
  pendingAction?: "replay" | "purge" | null;
  pendingMessageId?: number | null;
  onPurge?: (message: DeadLetterMessage) => void | Promise<void>;
  onReplay?: (message: DeadLetterMessage) => void | Promise<void>;
}

export default function QueueDeadLetterTable({
  messages,
  onPurge,
  onReplay,
  pendingAction = null,
  pendingMessageId = null,
}: QueueDeadLetterTableProps) {
  const hasActions = Boolean(onReplay || onPurge);
  const columns: readonly DataTableColumn<DeadLetterMessage>[] = [
    {
      id: "message",
      header: "Message",
      width: hasActions ? "52%" : "76%",
      cellComponent: ({ row }) => (
        <TitledCell
          subtitle={`${row.reason} · dead-lettered ${formatTimestamp(row.deadLetteredAt)}`}
        >
          {row.messageId}
        </TitledCell>
      ),
    },
    {
      id: "attempts",
      header: "Attempts",
      width: hasActions ? "16%" : "24%",
      cellComponent: ({ row }) => <span>{row.attempts}</span>,
    },
    ...(hasActions
      ? [
          {
            id: "actions",
            header: "Actions",
            width: "32%",
            cellComponent: ({ row }) => (
              <div class="queue-action-cell">
                {onReplay ? (
                  <Button
                    variant="secondary"
                    aria-busy={pendingAction === "replay" && pendingMessageId === row.messageId}
                    aria-label={`Replay message ${row.messageId}`}
                    onPress={() => onReplay(row)}
                    disabled={pendingMessageId === row.messageId}
                  >
                    <RefreshCwIcon size={15} />
                    {pendingAction === "replay" && pendingMessageId === row.messageId
                      ? "Replaying..."
                      : "Replay"}
                  </Button>
                ) : null}
                {onPurge ? (
                  <Button
                    variant="destructive"
                    aria-busy={pendingAction === "purge" && pendingMessageId === row.messageId}
                    aria-label={`Purge message ${row.messageId}`}
                    onPress={() => onPurge(row)}
                    disabled={pendingMessageId === row.messageId}
                  >
                    <Trash2Icon size={15} />
                    {pendingAction === "purge" && pendingMessageId === row.messageId
                      ? "Purging..."
                      : "Purge"}
                  </Button>
                ) : null}
              </div>
            ),
          } satisfies DataTableColumn<DeadLetterMessage>,
        ]
      : []),
  ];
  return (
    <DataTable<DeadLetterMessage>
      ariaLabel="Dead-letter queue messages"
      class="queue-resource-data-table"
      columns={columns}
      getKey={(message) => message.messageId}
      rows={messages}
    />
  );
}
