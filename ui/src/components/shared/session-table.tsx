import { For, Show } from "@askrjs/askr/control";
import type { ActiveSession } from "@/features/session/session-models";
import { formatTimestamp } from "@/shared/format";
import { QueryEmptyState } from "./query-state";

export interface SessionTableProps {
  sessions: ActiveSession[];
}

function reportedText(value: string | null | undefined) {
  return value && value.length > 0 ? value : "Unknown";
}

function messageCounts(session: ActiveSession) {
  if (session.messagesSent === undefined && session.messagesReceived === undefined) {
    return "Unknown";
  }

  return `${session.messagesSent ?? "--"} sent / ${session.messagesReceived ?? "--"} received`;
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
  const emptyState = (
    <QueryEmptyState
      title="No active sessions"
      description="No live broker or admin sessions are currently connected."
    />
  );

  const visibleSessions = sessions.slice(0, 500);

  return (
    <Show when={sessions.length > 0} fallback={emptyState}>
      <section class="session-list-section" aria-labelledby="live-sessions-title">
        <header class="session-list-section-header">
          <h2 id="live-sessions-title">Live sessions</h2>
          <p>Each item is one live broker or admin connection and the context reported for it.</p>
        </header>
        <ul class="session-list" aria-label="Live sessions">
          <For each={visibleSessions} by={(session) => session.key}>
            {(session) => (
              <li class="session-list-item">
                <div class="session-list-heading">
                  <strong class="session-list-id" title={session.sessionId ?? session.key}>
                    {session.sessionId ?? session.key}
                  </strong>
                  <span class="session-list-badge">
                    Route Family {session.routeFamily ?? "Unknown"}
                  </span>
                  <span class="session-list-badge">{session.transport ?? "Unknown"}</span>
                </div>
                <p class="session-list-description">
                  <span>{reportedText(session.subject)}</span>
                  <span aria-hidden="true">·</span>
                  <span>
                    {reportedText(session.identityClaim)}: {reportedText(session.identityValue)}
                  </span>
                </p>
                <dl class="session-list-metadata">
                  <div>
                    <dt>Remote</dt>
                    <dd class="session-table-cell-wrap">{session.remoteAddress ?? "Unknown"}</dd>
                  </div>
                  <div>
                    <dt>Connected</dt>
                    <dd>{formatTimestamp(session.connectedAt)}</dd>
                  </div>
                  <div>
                    <dt>Idle</dt>
                    <dd>{formatDuration(session.idleSeconds)}</dd>
                  </div>
                  <div>
                    <dt>Messages</dt>
                    <dd>{messageCounts(session)}</dd>
                  </div>
                </dl>
              </li>
            )}
          </For>
        </ul>
        <Show when={sessions.length > visibleSessions.length}>
          <p class="domain-table-limit-notice" role="status">
            Showing the first {visibleSessions.length} of {sessions.length} sessions. Narrow the
            active scope to inspect later sessions.
          </p>
        </Show>
      </section>
    </Show>
  );
}
