import { state } from "@askrjs/askr";
import { Button, Input, Label } from "@askrjs/ui";
import { AlertTriangleIcon } from "@askrjs/lucide";
import { Badge, EmptyState, SidebarLayout, Spinner } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import SessionTable from "@/components/shared/session-table";
import { createActiveSessionsQuery } from "@/features/session/session-query";
import { formatUnknownError } from "@/shared/errors/format";

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

    if (typeof window === "undefined") {
      return;
    }

    const nextRealm = realmInput().trim();
    const search = nextRealm ? `?realm=${encodeURIComponent(nextRealm)}` : "";
    window.location.assign(`/sessions${search}`);
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
                  if (typeof window !== "undefined") {
                    window.location.assign("/sessions");
                  }
                }}
              >
                Clear
              </Button>
            </div>
          </form>
        </section>

        {sessionsQuery.loading ? (
          <EmptyState
            class="domain-state"
            icon={<Spinner label="Loading" />}
            description="Loading active sessions..."
          />
        ) : null}

        {sessionsQuery.error ? (
          <EmptyState
            class="domain-state"
            icon={<AlertTriangleIcon size={18} />}
            description={formatUnknownError(sessionsQuery.error)}
          />
        ) : null}

        {data && !sessionsQuery.loading && !sessionsQuery.error ? (
          <>
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
          </>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
