import { For } from "@askrjs/askr";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import type { QueueInflightMessage } from "@/features/queue/queue-resource-models";

export interface QueueInflightTableProps {
  messages: QueueInflightMessage[];
}

function formatTimestamp(value: string) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString();
}

export default function QueueInflightTable({ messages }: QueueInflightTableProps) {
  return (
    <div class="domain-table-wrap">
      <Table class="domain-table">
        <TableHead>
          <TableRow>
            <TableHeaderCell>Message</TableHeaderCell>
            <TableHeaderCell>Family</TableHeaderCell>
            <TableHeaderCell>Attempts</TableHeaderCell>
            <TableHeaderCell>Session</TableHeaderCell>
            <TableHeaderCell>Expires</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={messages} by={(message) => message.messageId}>
            {(message) => (
              <TableRow>
                <TableCell>{message.messageId}</TableCell>
                <TableCell>{message.family}</TableCell>
                <TableCell>{message.attempts}</TableCell>
                <TableCell>{message.sessionId}</TableCell>
                <TableCell>{formatTimestamp(message.expiresAt)}</TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}
