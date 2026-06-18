import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import type { QueueInflightMessage } from "@/features/queue/queue-resource-models";
import { formatTimestamp } from "@/shared/format";

export interface QueueInflightTableProps {
  messages: QueueInflightMessage[];
}

export default function QueueInflightTable({ messages }: QueueInflightTableProps) {
  return (
    <div class="domain-table-wrap">
      <Table>
        <TableHead>
          <TableRow>
            <TableHeaderCell>Message</TableHeaderCell>
            <TableHeaderCell>Context</TableHeaderCell>
            <TableHeaderCell>Owner token</TableHeaderCell>
            <TableHeaderCell>Session</TableHeaderCell>
            <TableHeaderCell>Family</TableHeaderCell>
            <TableHeaderCell>Attempts</TableHeaderCell>
            <TableHeaderCell>Expires</TableHeaderCell>
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
                  <span class="domain-table-cell-truncate" title={`${message.realm} / ${message.area} / ${message.resource}`}>
                    {message.realm} / {message.area} / {message.resource}
                  </span>
                </TableCell>
                <TableCell>
                  <span class="domain-table-cell-truncate" title={message.inflightToken}>
                    {message.inflightToken}
                  </span>
                </TableCell>
                <TableCell>
                  <span class="domain-table-cell-truncate" title={message.sessionId}>
                    {message.sessionId}
                  </span>
                </TableCell>
                <TableCell>{message.family}</TableCell>
                <TableCell>{message.attempts}</TableCell>
                <TableCell>{formatTimestamp(message.expiresAt)}</TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}
