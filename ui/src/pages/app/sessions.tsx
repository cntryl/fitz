import { Stack } from "@askrjs/themes/layouts";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import SessionTable from "@/components/shared/session-table";
import { createActiveSessionsQuery } from "@/features/session/session-query";

export default function SessionsPage() {
  const sessionsQuery = createActiveSessionsQuery();
  const data = sessionsQuery.data;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Connection health"
          title="Active sessions"
          description="Inspect live broker and admin sessions with resolved route-family identity context."
          primaryAction={{
            label: "Refresh sessions",
            onPress: () => sessionsQuery.refresh(),
          }}
          status={{
            detail: "Disconnect destroys session state and reconnect creates a new session.",
            label: sessionsQuery.refreshing ? "Refreshing" : sessionsQuery.stale ? "Stale" : "Live",
            tone: sessionsQuery.refreshing ? "info" : sessionsQuery.stale ? "warning" : "success",
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
            {sessionsQuery.refreshing ? (
              <QueryRefreshingState description="Refreshing active sessions..." />
            ) : null}

            <DomainMetricTable
              title="Session summary"
              description="Live session count, resolved route families, and the longest idle connection."
              metrics={[
                { label: "Sessions", value: data.sessions.length, caption: "Current live sessions" },
                {
                  label: "Route families",
                  value: new Set(data.sessions.map((session) => session.routeFamily).filter(Boolean)).size,
                  caption: "Resolved families",
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
