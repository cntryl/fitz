import { Show } from "@askrjs/askr/control";
import { Block } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import DomainSummaryStrip from "@/components/shared/domain-summary-strip";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import SessionTable from "@/components/shared/session-table";
import { createActiveSessionsQuery } from "@/features/session/session-query";
import type { ActiveSession } from "@/features/session/session-models";
import { formatNumber } from "@/shared/format";

function countTransportKinds(sessions: ActiveSession[]) {
  return new Set(sessions.map((session) => session.transport ?? "Unknown")).size;
}

function longestIdleSeconds(sessions: ActiveSession[]) {
  const reported = sessions
    .map((session) => session.idleSeconds)
    .filter((idleSeconds): idleSeconds is number => idleSeconds !== undefined);

  return reported.length > 0 ? Math.max(...reported) : null;
}

export default function SessionsPage() {
  const sessionsQuery = createActiveSessionsQuery();
  const data = sessionsQuery.data;
  const sessions = data?.sessions ?? [];
  const transportKinds = countTransportKinds(sessions);
  const longestIdle = longestIdleSeconds(sessions);
  const isInitialLoad = sessionsQuery.loading && !data;
  const isInitialError = sessionsQuery.error && !data;

  // Session counts are shown below; the badge only reports abnormal data state.
  const headerStatus = isInitialLoad
    ? { label: "Loading", tone: "info" as const }
    : isInitialError
      ? { label: "Unavailable", tone: "danger" as const }
      : sessionsQuery.refreshing
        ? { label: "Refreshing", tone: "info" as const }
        : sessionsQuery.stale
          ? { label: "Stale", tone: "warning" as const }
          : undefined;

  return (
    <DomainPageFrame>
      <Block direction="column" gap="sm">
        <DomainHeader
          eyebrow="Connection health"
          title="Active sessions"
          description="Inspect currently connected broker and admin sessions and their reported context."
          primaryAction={{
            busy: sessionsQuery.refreshing,
            disabled: sessionsQuery.refreshing,
            label: "Refresh sessions",
            onPress: () => sessionsQuery.refresh(),
          }}
          status={headerStatus}
        />

        <Show when={!data && sessionsQuery.loading}>
          <QueryLoadingState
            title="Loading active sessions"
            description="Loading active sessions from the broker..."
          />
        </Show>

        <Show when={!data && sessionsQuery.error}>
          <QueryErrorState
            title="Unable to load active sessions"
            error={sessionsQuery.error}
            onRetry={() => sessionsQuery.refresh()}
          />
        </Show>

        <Show when={data}>
          {(data) => (
            <Block direction="column" gap="sm">
              <DomainSummaryStrip
                ariaLabel="Session summary"
                items={[
                  { label: "Sessions", value: data.sessions.length },
                  { label: "Transports", value: transportKinds },
                  {
                    label: "Longest idle",
                    value: longestIdle === null ? "Unknown" : `${formatNumber(longestIdle)}s`,
                  },
                ]}
              />

              <SessionTable sessions={data.sessions} />
            </Block>
          )}
        </Show>
      </Block>
    </DomainPageFrame>
  );
}
