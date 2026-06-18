import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
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
                <TableCell>{message.messageId}</TableCell>
                <TableCell>{message.family}</TableCell>
                <TableCell>{message.attempts}</TableCell>
                <TableCell>{formatTimestamp(message.deadLetteredAt)}</TableCell>
                <TableCell>{message.reason}</TableCell>
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
