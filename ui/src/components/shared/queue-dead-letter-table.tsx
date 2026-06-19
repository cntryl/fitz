import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { RefreshCwIcon, Trash2Icon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/controls";
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
  return (
    <div class="domain-table-wrap">
      <Table>
        <TableHead>
          <TableRow>
            <TableHeaderCell>Message</TableHeaderCell>
            <TableHeaderCell>Context</TableHeaderCell>
            <TableHeaderCell>Family</TableHeaderCell>
            <TableHeaderCell>Attempts</TableHeaderCell>
            <TableHeaderCell>Dead-lettered</TableHeaderCell>
            <TableHeaderCell>Reason</TableHeaderCell>
            {onReplay || onPurge ? <TableHeaderCell>Actions</TableHeaderCell> : null}
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={messages} by={(message) => message.messageId}>
            {(message) => (
              <TableRow>
                <TableCell>
                  <span class="domain-table-cell-truncate" title={String(message.messageId)}>
                    {message.messageId}
                  </span>
                </TableCell>
                <TableCell>
                  <span
                    class="domain-table-cell-truncate"
                    title={`${message.realm} / ${message.area} / ${message.resource}`}
                  >
                    {message.realm} / {message.area} / {message.resource}
                  </span>
                </TableCell>
                <TableCell>{message.family}</TableCell>
                <TableCell>{message.attempts}</TableCell>
                <TableCell>{formatTimestamp(message.deadLetteredAt)}</TableCell>
                <TableCell>
                  <span class="queue-dead-letter-reason" title={message.reason}>
                    {message.reason}
                  </span>
                </TableCell>
                {onReplay || onPurge ? (
                  <TableCell>
                    <div class="queue-action-cell">
                      {onReplay ? (
                        <Button
                          variant="secondary"
                          aria-busy={
                            pendingAction === "replay" && pendingMessageId === message.messageId
                          }
                          onPress={() => onReplay(message)}
                          disabled={pendingMessageId === message.messageId}
                        >
                          <RefreshCwIcon size={15} />
                          {pendingAction === "replay" && pendingMessageId === message.messageId
                            ? "Replaying..."
                            : "Replay"}
                        </Button>
                      ) : null}
                      {onPurge ? (
                        <Button
                          variant="destructive"
                          aria-busy={
                            pendingAction === "purge" && pendingMessageId === message.messageId
                          }
                          onPress={() => onPurge(message)}
                          disabled={pendingMessageId === message.messageId}
                        >
                          <Trash2Icon size={15} />
                          {pendingAction === "purge" && pendingMessageId === message.messageId
                            ? "Purging..."
                            : "Purge"}
                        </Button>
                      ) : null}
                    </div>
                  </TableCell>
                ) : null}
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}
