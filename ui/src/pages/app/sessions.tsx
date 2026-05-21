import { state } from "@askrjs/askr";
import { navigate } from "@askrjs/askr/router";
import { Input, Label } from "@askrjs/ui";
import { Button } from "@askrjs/themes/controls";
import { Badge } from "@askrjs/themes/surfaces";
import DomainHeader from "@/components/shared/domain-header";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import SidebarLayout from "@/components/shared/sidebar-layout";
import SessionTable from "@/components/shared/session-table";
import { createActiveSessionsQuery } from "@/features/session/session-query";

function currentRealmFilter() {
  if (typeof window === "undefined") {
    return "";
  }

  return new URLSearchParams(window.location.search).get("realm") ?? "";
}

export default function SessionsPage() {
  const realmFilter = currentRealmFilter();
  const realmInput = state(realmFilter);
  const sessionsQuery = createActiveSessionsQuery(realmFilter || undefined);
  const data = sessionsQuery.data;

  const sidebar = createDomainSidebar({
    data,
    title: "Active sessions",
    description: "Current broker and admin session coverage.",
    stats: (current) => [
      { label: "Sessions", value: current.sessions.length },
      {
        label: "Realm scope",
        value: realmFilter || "All realms",
        note: "Optional backend filter",
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
      <div class="admin-sidebar-actions">
        <Button class="secondary-action" onPress={() => sessionsQuery.refresh()}>
          Refresh
        </Button>
      </div>
    ),
  });

  async function onFilterSubmit(event: Event) {
    event.preventDefault();

    const nextRealm = realmInput().trim();
    const search = nextRealm ? `?realm=${encodeURIComponent(nextRealm)}` : "";
    navigate(`/sessions${search}`);
  }

  return (
    <SidebarLayout
      sidebar={sidebar}
      sidebarPosition="end"
      sidebarWidth="18rem"
      gap="1.5rem"
      collapseBelow="md"
    >
      <section class="domain-page">
        <DomainHeader
          domain="Sessions"
          title="Active sessions"
          description="Inspect live broker and admin sessions, with optional realm scoping."
          onRefresh={() => sessionsQuery.refresh()}
        />

        <section class="domain-section">
          <div class="domain-section-header">
            <div>
              <p class="eyebrow">Filter</p>
              <h2>Realm scope</h2>
            </div>
          </div>

          <form class="session-filter" onSubmit={onFilterSubmit}>
            <div class="auth-field">
              <Label for="realm-filter">Realm</Label>
              <Input
                id="realm-filter"
                value={realmInput()}
                onInput={(event: Event) => realmInput.set((event.target as HTMLInputElement).value)}
                placeholder="Leave blank for all realms"
              />
            </div>

            <div class="session-filter-actions">
              <Button type="submit" class="primary-action">
                Apply filter
              </Button>
              <Button
                class="secondary-action"
                onPress={() => {
                  realmInput.set("");
                  navigate("/sessions");
                }}
              >
                Clear
              </Button>
            </div>
          </form>
        </section>

        {sessionsQuery.loading ? (
          <QueryLoadingState description="Loading active sessions..." />
        ) : null}

        {sessionsQuery.error ? (
          <QueryErrorState error={sessionsQuery.error} />
        ) : null}

        {data && !sessionsQuery.loading && !sessionsQuery.error ? (
          <div class="domain-stack">
            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <p class="eyebrow">Overview</p>
                  <h2>{data.sessions.length} sessions</h2>
                </div>
                <Badge>{realmFilter || "All realms"}</Badge>
              </div>
            </section>

            <SessionTable sessions={data.sessions} />
          </div>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
