import { state } from "@askrjs/askr";
import { navigate } from "@askrjs/askr/router";
import { Input, Label } from "@askrjs/ui";
import { Button } from "@askrjs/themes/controls";
import { Inline, Section, Stack } from "@askrjs/themes/layouts";
import { Badge } from "@askrjs/themes/surfaces";
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

function currentRealmFilter() {
  if (typeof window === "undefined") {
    return "";
  }

  return new URLSearchParams(window.location.search).get("realm") ?? "";
}

export default function SessionsPage() {
  const realmFilter = currentRealmFilter();
  const [realmInput, setRealmInput] = state(realmFilter);
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
      <Stack gap="3">
        <Button onPress={() => sessionsQuery.refresh()}>Refresh</Button>
      </Stack>
    ),
  });

  async function onFilterSubmit(event: Event) {
    event.preventDefault();

    const nextRealm = realmInput().trim();
    const search = nextRealm ? `?realm=${encodeURIComponent(nextRealm)}` : "";
    navigate(`/sessions${search}`);
  }

  return (
    <DomainPageFrame sidebar={sidebar}>
      <Stack gap="3">
        <DomainHeader
          domain="Sessions"
          title="Active sessions"
          description="Inspect live broker and admin sessions, with optional realm scoping."
          onRefresh={() => sessionsQuery.refresh()}
        />

        <Section size="3">
          <div class="domain-section-header">
            <div>
              <p class="eyebrow">Filter</p>
              <h2>Realm scope</h2>
            </div>
          </div>

          <Stack asChild gap="3">
            <form onSubmit={onFilterSubmit}>
            <div class="auth-field">
              <Label for="realm-filter">Realm</Label>
              <Input
                id="realm-filter"
                value={realmInput()}
                onInput={(event: Event) => setRealmInput((event.target as HTMLInputElement).value)}
                placeholder="Leave blank for all realms"
              />
            </div>

            <Inline gap="3" wrap="wrap">
              <Button type="submit">Apply filter</Button>
              <Button
                onPress={() => {
                  setRealmInput("");
                  navigate("/sessions");
                }}
              >
                Clear
              </Button>
            </Inline>
            </form>
          </Stack>
        </Section>

        {!data && sessionsQuery.loading ? (
          <QueryLoadingState description="Loading active sessions..." />
        ) : null}

        {!data && sessionsQuery.error ? <QueryErrorState error={sessionsQuery.error} /> : null}

        {data ? (
          <Stack gap="3">
            {sessionsQuery.refreshing ? (
              <QueryRefreshingState description="Refreshing active sessions..." />
            ) : null}

            <Section size="3">
              <div class="domain-section-header">
                <div>
                  <p class="eyebrow">Overview</p>
                  <h2>{data.sessions.length} sessions</h2>
                </div>
                <Badge>{realmFilter || "All realms"}</Badge>
              </div>
            </Section>

            <SessionTable sessions={data.sessions} />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
