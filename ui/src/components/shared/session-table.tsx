import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import type { ActiveSession } from "@/features/session/session-models";
import { formatTimestamp } from "@/shared/format";

export interface SessionTableProps {
  sessions: ActiveSession[];
}

function formatDuration(value?: number) {
  if (value == null) {
    return "Unknown";
  }

  if (value < 60) {
    return `${value}s`;
  }

  const minutes = Math.floor(value / 60);

  if (minutes < 60) {
    return `${minutes}m`;
  }

  const hours = Math.floor(minutes / 60);
  return `${hours}h`;
}

export default function SessionTable({ sessions }: SessionTableProps) {
  return (
    <div class="domain-table-wrap">
      <Table class="domain-table">
        <TableHead>
          <TableRow>
            <TableHeaderCell>Session</TableHeaderCell>
            <TableHeaderCell>Realm</TableHeaderCell>
            <TableHeaderCell>Transport</TableHeaderCell>
            <TableHeaderCell>Remote address</TableHeaderCell>
            <TableHeaderCell>Connected</TableHeaderCell>
            <TableHeaderCell>Idle</TableHeaderCell>
            <TableHeaderCell>Messages</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <For each={sessions} by={(session) => session.key}>
            {(session) => (
              <TableRow>
                <TableCell>{session.sessionId ?? session.key}</TableCell>
                <TableCell>{session.realm ?? "All realms"}</TableCell>
                <TableCell>{session.transport ?? "Unknown"}</TableCell>
                <TableCell>{session.remoteAddress ?? "Unknown"}</TableCell>
                <TableCell>{formatTimestamp(session.connectedAt)}</TableCell>
                <TableCell>{formatDuration(session.idleSeconds)}</TableCell>
                <TableCell>
                  {session.messagesSent ?? 0} sent / {session.messagesReceived ?? 0} received
                </TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>
    </div>
  );
}
