import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Card, CardContent } from "@askrjs/themes/surfaces";
import type { QueueInflightMessage } from "@/features/queue/queue-resource-models";
import { formatTimestamp } from "@/shared/format";

export interface QueueInflightTableProps {
  messages: QueueInflightMessage[];
}

export default function QueueInflightTable({ messages }: QueueInflightTableProps) {
  return (
    <Card class="domain-table-card" padding="sm" variant="default">
      <CardContent>
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
      </CardContent>
    </Card>
  );
}
