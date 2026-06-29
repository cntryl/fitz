import { For } from "@askrjs/askr/control";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/components";
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

  const columns: readonly VirtualTableColumn<ActiveSession>[] = [
    {
      id: "session",
      header: "Session ID",
      width: "15%",
      cellComponent: ({ row }) => (
        <span class="session-table-cell-truncate" title={row.sessionId ?? row.key}>
          {row.sessionId ?? row.key}
        </span>
      ),
    },
    {
      id: "route-family",
      header: "Route family",
      width: "8%",
      cellComponent: ({ row }) => <span>{row.routeFamily ?? "Unknown"}</span>,
    },
    {
      id: "subject",
      header: "Subject",
      width: "11%",
      cellComponent: ({ row }) => (
        <span class="session-table-cell-truncate" title={row.subject || "Unauthenticated"}>
          {row.subject || "Unauthenticated"}
        </span>
      ),
    },
    {
      id: "identity-claim",
      header: "Identity claim",
      width: "10%",
      cellComponent: ({ row }) => (
        <span class="session-table-cell-truncate" title={row.identityClaim ?? "Not resolved"}>
          {row.identityClaim ?? "Not resolved"}
        </span>
      ),
    },
    {
      id: "identity-value",
      header: "Identity value",
      width: "11%",
      cellComponent: ({ row }) => (
        <span class="session-table-cell-truncate" title={row.identityValue ?? "Not resolved"}>
          {row.identityValue ?? "Not resolved"}
        </span>
      ),
    },
    {
      id: "transport",
      header: "Transport",
      width: "7%",
      cellComponent: ({ row }) => <span>{row.transport ?? "Unknown"}</span>,
    },
    {
      id: "remote",
      header: "Remote address",
      width: "12%",
      cellComponent: ({ row }) => (
        <span class="session-table-cell-truncate" title={row.remoteAddress ?? "Unknown"}>
          {row.remoteAddress ?? "Unknown"}
        </span>
      ),
    },
    {
      id: "connected",
      header: "Connected at",
      width: "10%",
      cellComponent: ({ row }) => (
        <span class="session-table-cell-truncate" title={formatTimestamp(row.connectedAt)}>
          {formatTimestamp(row.connectedAt)}
        </span>
      ),
    },
    {
      id: "idle",
      header: "Idle",
      width: "6%",
      cellComponent: ({ row }) => <span>{formatDuration(row.idleSeconds)}</span>,
    },
    {
      id: "messages",
      header: "Messages",
      width: "10%",
      cellComponent: ({ row }) => (
        <span>
          {row.messagesSent ?? 0} sent / {row.messagesReceived ?? 0} received
        </span>
      ),
    },
  ];
  const tableHeight = Math.min(520, Math.max(176, 44 + sessions.length * 48));

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
            <VirtualTable<ActiveSession>
              aria-label="Live sessions"
              class="session-virtual-table"
              columns={columns}
              getKey={(session) => session.key}
              headerHeight={44}
              overscan={10}
              rowHeight={48}
              rows={sessions}
              style={{ height: `${tableHeight}px` }}
            />
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
                        <dd class="session-table-cell-wrap">
                          {session.remoteAddress ?? "Unknown"}
                        </dd>
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
                          {session.messagesSent ?? 0} sent / {session.messagesReceived ?? 0}{" "}
                          received
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
