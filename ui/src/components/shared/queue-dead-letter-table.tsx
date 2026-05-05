import { For } from "@askrjs/askr";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import type { DeadLetterMessage } from "@/features/queue/queue-models";

export interface QueueDeadLetterTableProps {
  messages: DeadLetterMessage[];
}

function formatTimestamp(value: string) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString();
}

export default function QueueDeadLetterTable({ messages }: QueueDeadLetterTableProps) {
  return (
    <div class="domain-table-wrap">
      <Table class="domain-table">
        <TableHead>
          <TableRow>
            <TableHeaderCell>Message</TableHeaderCell>
            <TableHeaderCell>Family</TableHeaderCell>
            <TableHeaderCell>Attempts</TableHeaderCell>
            <TableHeaderCell>Dead-lettered</TableHeaderCell>
            <TableHeaderCell>Reason</TableHeaderCell>
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
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}
