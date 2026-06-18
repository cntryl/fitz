import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import type { ActiveSession } from "@/features/session/session-models";
import { formatTimestamp } from "@/shared/format";
import { QueryEmptyState } from "./query-state";

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
  if (sessions.length === 0) {
    return (
      <QueryEmptyState
        title="No active sessions"
        description="No live broker or admin sessions are currently connected."
      />
    );
  }

  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>Live sessions</CardTitle>
        <CardDescription>
          Each row is one live broker or admin connection. Route family, identity claim, and idle
          time show how the session is resolved.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div class="domain-table-wrap">
          <Table>
            <TableHead>
              <TableRow>
                <TableHeaderCell>Session</TableHeaderCell>
                <TableHeaderCell>Route family</TableHeaderCell>
                <TableHeaderCell>Subject</TableHeaderCell>
                <TableHeaderCell>Identity claim</TableHeaderCell>
                <TableHeaderCell>Transport</TableHeaderCell>
                <TableHeaderCell>Remote address</TableHeaderCell>
                <TableHeaderCell>Connected at</TableHeaderCell>
                <TableHeaderCell>Idle</TableHeaderCell>
                <TableHeaderCell>Messages</TableHeaderCell>
              </TableRow>
            </TableHead>
            <TableBody>
              <For each={sessions} by={(session) => session.key}>
                {(session) => (
                  <TableRow>
                    <TableCell>{session.sessionId ?? session.key}</TableCell>
                    <TableCell>{session.routeFamily ?? "Unknown"}</TableCell>
                    <TableCell>{session.subject || "Unauthenticated"}</TableCell>
                    <TableCell>
                      {session.identityClaim && session.identityValue
                        ? `${session.identityClaim}=${session.identityValue}`
                        : "Not resolved"}
                    </TableCell>
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
      </CardContent>
    </Card>
  );
}
