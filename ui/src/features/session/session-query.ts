import { createQuery } from "@askrjs/askr/data";
import { sessionService } from "./session-service";
import type { ActiveSessionsOverview } from "./session-models";

const CURRENT_SESSION_KEY = "session:current";
function activeSessionsKey(realm?: string) {
  return `session:active:${realm ?? "all"}`;
}

export function createCurrentSessionQuery() {
  return createQuery({
    key: CURRENT_SESSION_KEY,
    fetch: ({ signal }) => sessionService.getCurrentSession({ signal }),
  });
}

export function createActiveSessionsQuery(realm?: string) {
  return createQuery<ActiveSessionsOverview>({
    key: activeSessionsKey(realm),
    fetch: ({ signal }) => sessionService.listActiveSessions(realm, { signal }),
  });
}
