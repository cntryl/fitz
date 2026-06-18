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

function countTransportKinds(sessions: ActiveSession[]) {
  return new Set(sessions.map((session) => session.transport ?? "Unknown")).size;
}

function longestIdleSeconds(sessions: ActiveSession[]) {
  return sessions.reduce((max, session) => Math.max(max, session.idleSeconds ?? 0), 0);
}

function describeIdleRisk(idleSeconds: number) {
  if (idleSeconds >= 300) {
    return "High";
  }

  if (idleSeconds >= 120) {
    return "Moderate";
  }

  return "Low";
}

function summarizeSessions(sessions: ActiveSession[]): SessionsPostureSummary {
  if (sessions.length === 0) {
    return {
      detail: "No active sessions are visible. The broker is not holding any live connections right now.",
      label: "Idle",
      tone: "success",
    };
  }

  const identityGaps = sessions.filter((session) => !session.identityClaim || !session.identityValue).length;
  const unauthenticated = sessions.filter((session) => !session.subject).length;
  const unresolvedSessions = countUnresolvedSessions(sessions);
  const longestIdle = longestIdleSeconds(sessions);
  const routeFamilies = countResolvedRouteFamilies(sessions);
  const transportKinds = countTransportKinds(sessions);
  const summary = `${countLabel(sessions.length, "session")} across ${countLabel(routeFamilies, "route family", "route families")} and ${countLabel(transportKinds, "transport")}. Longest idle: ${formatNumber(longestIdle)}s.`;

  if (identityGaps > 0 || unauthenticated > 0) {
    return {
      detail: `${summary} ${countLabel(unresolvedSessions, "session")} still need identity or subject resolution.`,
      label: "Attention",
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
    tone: "success",
  };
}

export default function SessionsPage() {
  const sessionsQuery = createActiveSessionsQuery();
  const data = sessionsQuery.data;
  const sessions = data?.sessions ?? [];
  const routeFamilies = countResolvedRouteFamilies(sessions);
  const transportKinds = countTransportKinds(sessions);
  const longestIdle = longestIdleSeconds(sessions);
  const idleRisk = describeIdleRisk(longestIdle);
  const posture = data ? summarizeSessions(sessions) : null;
  const isInitialLoad = sessionsQuery.loading && !data;
  const isInitialError = sessionsQuery.error && !data;

  const headerStatus = isInitialLoad
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
              : posture?.label ?? "Healthy",
          tone: sessionsQuery.refreshing
            ? "info"
            : sessionsQuery.stale
              ? "warning"
              : posture?.tone ?? "success",
        };

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
            detail: headerStatus.detail,
            label: headerStatus.label,
            tone: headerStatus.tone,
          }}
        />

        {!data && sessionsQuery.loading ? (
          <QueryLoadingState
            title="Loading active sessions"
            description="Loading active sessions from the broker..."
          />
        ) : null}

        {!data && sessionsQuery.error ? (
          <QueryErrorState
            title="Unable to load active sessions"
            error={sessionsQuery.error}
            onRetry={() => sessionsQuery.refresh()}
          />
        ) : null}

        {data ? (
          <Stack gap="3">
            <DomainMetricTable
              title="Session summary"
              description="Current live sessions, route-family coverage, transport mix, and idle risk."
              metrics={[
                { label: "Sessions", value: data.sessions.length, caption: "Current live sessions" },
                {
                  label: "Route families",
                  value: routeFamilies,
                  caption: "Resolved families",
                },
                {
                  label: "Transports",
                  value: transportKinds,
                  caption: "Distinct transport types",
                },
                {
                  label: "Idle risk",
                  value: idleRisk,
                  caption:
                    sessions.length > 0 ? `${formatNumber(longestIdle)}s max idle` : "No live sessions",
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
