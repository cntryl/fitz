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

type SessionsTone = "info" | "success" | "warning" | "danger";

interface SessionsPostureSummary {
  detail: string;
  label: string;
  tone: SessionsTone;
}

function countLabel(value: number, singular: string, plural = `${singular}s`) {
  return `${formatNumber(value)} ${value === 1 ? singular : plural}`;
}

function countResolvedRouteFamilies(sessions: ActiveSession[]) {
  return new Set(
    sessions
      .map((session) => session.routeFamily)
      .filter((routeFamily): routeFamily is number => routeFamily != null),
  ).size;
}

function countTransportKinds(sessions: ActiveSession[]) {
  return new Set(sessions.map((session) => session.transport ?? "Unknown")).size;
}

function longestIdleSeconds(sessions: ActiveSession[]) {
  const reported = sessions
    .map((session) => session.idleSeconds)
    .filter((idleSeconds): idleSeconds is number => idleSeconds !== undefined);

  return reported.length > 0 ? Math.max(...reported) : null;
}

function summarizeSessions(sessions: ActiveSession[]): SessionsPostureSummary {
  if (sessions.length === 0) {
    return {
      detail:
        "No active sessions are visible. The broker is not holding any live connections right now.",
      label: "Idle",
      tone: "success",
    };
  }

  const longestIdle = longestIdleSeconds(sessions);
  const routeFamilies = countResolvedRouteFamilies(sessions);
  const transportKinds = countTransportKinds(sessions);
  const idleSummary =
    longestIdle === null
      ? "Idle duration is not reported."
      : `Longest reported idle: ${formatNumber(longestIdle)}s.`;
  const summary = `${countLabel(sessions.length, "session")} across ${countLabel(routeFamilies, "route family", "route families")} and ${countLabel(transportKinds, "transport")}. ${idleSummary}`;

  return {
    detail: summary,
    label: "Active",
    tone: "info",
  };
}

export default function SessionsPage() {
  const sessionsQuery = createActiveSessionsQuery();
  const data = sessionsQuery.data;
  const sessions = data?.sessions ?? [];
  const routeFamilies = countResolvedRouteFamilies(sessions);
  const transportKinds = countTransportKinds(sessions);
  const longestIdle = longestIdleSeconds(sessions);
  const posture = data ? summarizeSessions(sessions) : null;
  const isInitialLoad = sessionsQuery.loading && !data;
  const isInitialError = sessionsQuery.error && !data;

  const headerStatus: {
    detail: string;
    label: string;
    tone: "info" | "success" | "warning" | "danger";
  } = isInitialLoad
    ? {
        detail: "Loading active sessions from broker telemetry.",
        label: "Loading",
        tone: "info",
      }
    : isInitialError
      ? {
          detail: "Could not load active sessions from this route.",
          label: "Unavailable",
          tone: "danger",
        }
      : {
          detail: posture?.detail ?? "Live sessions reflect currently connected clients only.",
          label: sessionsQuery.refreshing
            ? "Refreshing"
            : sessionsQuery.stale
              ? "Stale"
              : (posture?.label ?? "Healthy"),
          tone: sessionsQuery.refreshing
            ? "info"
            : sessionsQuery.stale
              ? "warning"
              : (posture?.tone ?? "success"),
        };

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
          status={{
            detail: headerStatus.detail,
            label: headerStatus.label,
            tone: headerStatus.tone,
          }}
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
                title="Session summary"
                items={[
                  { label: "Sessions", value: data.sessions.length },
                  { label: "Route families", value: routeFamilies },
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
