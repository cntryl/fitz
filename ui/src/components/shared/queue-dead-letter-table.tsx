import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
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
  const columns: readonly VirtualTableColumn<DeadLetterMessage>[] = [
    {
      id: "message",
      header: "Message",
      width: hasActions ? "10%" : "14%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={String(row.messageId)}>
          {row.messageId}
        </span>
      ),
    },
    {
      id: "context",
      header: "Context",
      width: hasActions ? "18%" : "24%",
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
      id: "family",
      header: "Family",
      width: hasActions ? "7%" : "10%",
      cellComponent: ({ row }) => <span>{row.family}</span>,
    },
    {
      id: "attempts",
      header: "Attempts",
      width: hasActions ? "7%" : "10%",
      cellComponent: ({ row }) => <span>{row.attempts}</span>,
    },
    {
      id: "dead-lettered",
      header: "Dead-lettered",
      width: hasActions ? "15%" : "18%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={formatTimestamp(row.deadLetteredAt)}>
          {formatTimestamp(row.deadLetteredAt)}
        </span>
      ),
    },
    {
      id: "reason",
      header: "Reason",
      width: hasActions ? "19%" : "24%",
      cellComponent: ({ row }) => (
        <span class="queue-dead-letter-reason" title={row.reason}>
          {row.reason}
        </span>
      ),
    },
    ...(hasActions
      ? [
          {
            id: "actions",
            header: "Actions",
            width: "24%",
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
          } satisfies VirtualTableColumn<DeadLetterMessage>,
        ]
      : []),
  ];
  const tableHeight = Math.min(480, Math.max(144, 44 + messages.length * 52));

  return (
    <VirtualTable<DeadLetterMessage>
      aria-label="Dead-letter queue messages"
      class="queue-resource-virtual-table"
      columns={columns}
      getKey={(message) => message.messageId}
      headerHeight={44}
      overscan={8}
      rowHeight={52}
      rows={messages}
      style={{ height: `${tableHeight}px` }}
    />
  );
}
