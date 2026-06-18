import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
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
          <div class="session-table-desktop">
            <Table>
              <TableHead>
                <TableRow>
                  <TableHeaderCell>Session ID</TableHeaderCell>
                  <TableHeaderCell>Route family</TableHeaderCell>
                  <TableHeaderCell>Subject</TableHeaderCell>
                  <TableHeaderCell>Identity claim</TableHeaderCell>
                  <TableHeaderCell>Identity value</TableHeaderCell>
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
                      <TableCell>
                        <span
                          class="session-table-cell-truncate"
                          title={session.sessionId ?? session.key}
                        >
                          {session.sessionId ?? session.key}
                        </span>
                      </TableCell>
                      <TableCell>{session.routeFamily ?? "Unknown"}</TableCell>
                      <TableCell>{session.subject || "Unauthenticated"}</TableCell>
                      <TableCell>{session.identityClaim ?? "Not resolved"}</TableCell>
                      <TableCell>{session.identityValue ?? "Not resolved"}</TableCell>
                      <TableCell>{session.transport ?? "Unknown"}</TableCell>
                      <TableCell>
                        <span class="session-table-cell-wrap" title={session.remoteAddress ?? "Unknown"}>
                          {session.remoteAddress ?? "Unknown"}
                        </span>
                      </TableCell>
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

          <div class="session-table-mobile">
            <ul class="session-mobile-list">
              <For each={sessions} by={(session) => session.key}>
                {(session) => (
                  <li class="session-mobile-row">
                    <dl class="session-mobile-grid">
                      <div>
                        <dt>Session ID</dt>
                        <dd>
                          <span
                            class="session-table-cell-truncate"
                            title={session.sessionId ?? session.key}
                          >
                            {session.sessionId ?? session.key}
                          </span>
                        </dd>
                      </div>

                      <div>
                        <dt>Subject</dt>
                        <dd>{session.subject || "Unauthenticated"}</dd>
                      </div>

                      <div>
                        <dt>Identity claim</dt>
                        <dd>{session.identityClaim ?? "Not resolved"}</dd>
                      </div>

                      <div>
                        <dt>Identity value</dt>
                        <dd>{session.identityValue ?? "Not resolved"}</dd>
                      </div>

                      <div>
                        <dt>Route family</dt>
                        <dd>{session.routeFamily ?? "Unknown"}</dd>
                      </div>

                      <div>
                        <dt>Transport</dt>
                        <dd>{session.transport ?? "Unknown"}</dd>
                      </div>

                      <div>
                        <dt>Remote address</dt>
                        <dd class="session-table-cell-wrap">{session.remoteAddress ?? "Unknown"}</dd>
                      </div>

                      <div>
                        <dt>Connected at</dt>
                        <dd>{formatTimestamp(session.connectedAt)}</dd>
                      </div>

                      <div>
                        <dt>Idle</dt>
                        <dd>{formatDuration(session.idleSeconds)}</dd>
                      </div>

                      <div>
                        <dt>Messages</dt>
                        <dd>
                          {session.messagesSent ?? 0} sent / {session.messagesReceived ?? 0} received
                        </dd>
                      </div>
                    </dl>
                  </li>
                )}
              </For>
            </ul>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
