import { Button } from "@askrjs/themes/controls";
import { Stack } from "@askrjs/themes/layouts";
import DomainHeader from "@/components/shared/domain-header";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
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

  const sidebar = createDomainSidebar({
    data,
    title: "Active sessions",
    description: "Current broker and admin session coverage.",
    stats: (current) => [
      { label: "Sessions", value: current.sessions.length },
      {
        label: "Route families",
        value: new Set(current.sessions.map((session) => session.routeFamily).filter(Boolean))
          .size,
        note: "Resolved",
      },
      {
        label: "Longest idle",
        value:
          current.sessions.reduce((max, session) => Math.max(max, session.idleSeconds ?? 0), 0) ||
          0,
        note: "Seconds",
      },
    ],
    footer: (
      <Stack gap="3">
        <Button onPress={() => sessionsQuery.refresh()}>Refresh</Button>
      </Stack>
    ),
  });

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          title="Active sessions"
          description="Inspect live broker and admin sessions with resolved route-family identity context."
          onRefresh={() => sessionsQuery.refresh()}
        />

        {!data && sessionsQuery.loading ? (
          <QueryLoadingState description="Loading active sessions..." />
        ) : null}

        {!data && sessionsQuery.error ? <QueryErrorState error={sessionsQuery.error} /> : null}

        {data ? (
          <Stack gap="3">
            {sessionsQuery.refreshing ? (
              <QueryRefreshingState description="Refreshing active sessions..." />
            ) : null}

            <SessionTable sessions={data.sessions} />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
