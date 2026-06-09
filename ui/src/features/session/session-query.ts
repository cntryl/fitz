import { createQuery } from "@askrjs/askr/data";
import { sessionService } from "./session-service";
import type { ActiveSessionsOverview, SessionState } from "./session-models";

const CURRENT_SESSION_KEY = "session:current";
const ACTIVE_SESSIONS_KEY = "session:active";

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

export function createActiveSessionsQuery() {
  return createQuery<ActiveSessionsOverview>({
    key: ACTIVE_SESSIONS_KEY,
    fetch: ({ signal }) => sessionService.listActiveSessions({ signal }),
  });
}
