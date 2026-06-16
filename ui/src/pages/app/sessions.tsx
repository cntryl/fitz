import { Stack } from "@askrjs/themes/layouts";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import SessionTable from "@/components/shared/session-table";
import { createActiveSessionsQuery } from "@/features/session/session-query";
import type { ActiveSession } from "@/features/session/session-models";
import { formatNumber } from "@/shared/format";

type SessionsTone = "info" | "success" | "warning" | "danger";

interface SessionsPostureSummary {
  detail: string;
  label: string;
  nextStep: string;
  tone: SessionsTone;
}

function countLabel(value: number, singular: string, plural = `${singular}s`) {
  return `${formatNumber(value)} ${value === 1 ? singular : plural}`;
}

function countResolvedRouteFamilies(sessions: ActiveSession[]) {
  return new Set(
    sessions.map((session) => session.routeFamily).filter((routeFamily): routeFamily is number => routeFamily != null),
  ).size;
}

function countUnresolvedSessions(sessions: ActiveSession[]) {
  return sessions.filter(
    (session) => !session.identityClaim || !session.identityValue || !session.subject,
  ).length;
}

function summarizeSessions(sessions: ActiveSession[]): SessionsPostureSummary {
  if (sessions.length === 0) {
    return {
      detail: "No active sessions are visible. The broker is not holding any live connections right now.",
      label: "Idle",
      nextStep: "Refresh if you expected clients, or inspect the broker dashboard for broader process health.",
      tone: "success",
    };
  }

  const identityGaps = sessions.filter((session) => !session.identityClaim || !session.identityValue).length;
  const unauthenticated = sessions.filter((session) => !session.subject).length;
  const unresolvedSessions = countUnresolvedSessions(sessions);
  const longestIdle = sessions.reduce((max, session) => Math.max(max, session.idleSeconds ?? 0), 0);
  const routeFamilies = countResolvedRouteFamilies(sessions);
  const transportKinds = new Set(sessions.map((session) => session.transport ?? "Unknown")).size;
  const summary = `${countLabel(sessions.length, "session")} across ${countLabel(routeFamilies, "route family", "route families")} and ${countLabel(transportKinds, "transport")}. Longest idle: ${formatNumber(longestIdle)}s.`;

  if (identityGaps > 0 || unauthenticated > 0) {
    return {
      detail: `${summary} ${countLabel(unresolvedSessions, "session")} still need identity or subject resolution.`,
      label: "Attention",
      nextStep: "Inspect the table for Not resolved identity claims and unauthenticated subjects first.",
      tone: "danger",
    };
  }

  if (longestIdle >= 300) {
    return {
      detail: `${summary} One or more sessions have been idle for 5 minutes or longer.`,
      label: "Stale",
      nextStep: "Inspect the longest-idle sessions for transport or application-level stalls.",
      tone: "warning",
    };
  }

  return {
    detail: `${summary} Connections look healthy and identity is resolved.`,
    label: "Healthy",
    nextStep: "Use the table to inspect transport, remote address, and message counts if you need more detail.",
    tone: "success",
  };
}

export default function SessionsPage() {
  const sessionsQuery = createActiveSessionsQuery();
  const data = sessionsQuery.data;
  const sessions = data?.sessions ?? [];
  const unresolvedSessions = countUnresolvedSessions(sessions);
  const posture = data ? summarizeSessions(sessions) : null;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Connection health"
          title="Active sessions"
          description="Inspect live broker and admin sessions, then drill into route family, identity, and idle health."
          primaryAction={{
            label: "Refresh sessions",
            onPress: () => sessionsQuery.refresh(),
          }}
          status={{
            detail: posture?.detail ?? "Disconnect destroys session state and reconnect creates a new session.",
            label: sessionsQuery.refreshing
              ? "Refreshing"
              : sessionsQuery.stale
                ? "Stale"
                : posture?.label ?? "Live",
            tone: sessionsQuery.refreshing
              ? "info"
              : sessionsQuery.stale
                ? "warning"
                : posture?.tone ?? "success",
          }}
        />

        {!data && sessionsQuery.loading ? (
          <QueryLoadingState description="Loading active sessions..." />
        ) : null}

        {!data && sessionsQuery.error ? (
          <QueryErrorState error={sessionsQuery.error} onRetry={() => sessionsQuery.refresh()} />
        ) : null}

        {data ? (
          <Stack gap="3">
            <DomainMetricTable
              title="Session summary"
              description="Live session count, route-family coverage, identity gaps, and the longest idle connection."
              metrics={[
                { label: "Sessions", value: data.sessions.length, caption: "Current live sessions" },
                {
                  label: "Route families",
                  value: countResolvedRouteFamilies(data.sessions),
                  caption: "Resolved families",
                },
                {
                  label: "Unresolved sessions",
                  value: unresolvedSessions,
                  caption: "Identity or subject gaps",
                },
                {
                  label: "Longest idle",
                  value:
                    data.sessions.reduce(
                      (max, session) => Math.max(max, session.idleSeconds ?? 0),
                      0,
                    ) || 0,
                  caption: "Seconds",
                },
              ]}
            />

            <SessionTable sessions={data.sessions} />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
