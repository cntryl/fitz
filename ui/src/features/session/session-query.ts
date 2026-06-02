import { createQuery } from "@askrjs/askr/data";
import { sessionService } from "./session-service";
import type { ActiveSessionsOverview, SessionState } from "./session-models";

const CURRENT_SESSION_KEY = "session:current";
function activeSessionsKey(realm?: string) {
  return `session:active:${realm ?? "all"}`;
}

export function createCurrentSessionQuery() {
  return createQuery<SessionState>({
    key: CURRENT_SESSION_KEY,
    fetch: async ({ signal }) =>
      (await sessionService.getCurrentSession({ signal })) ?? {
        authenticated: false,
        username: "admin",
      },
  });
}

export function createActiveSessionsQuery(realm?: string) {
  return createQuery<ActiveSessionsOverview>({
    key: activeSessionsKey(realm),
    fetch: ({ signal }) => sessionService.listActiveSessions(realm, { signal }),
  });
}
