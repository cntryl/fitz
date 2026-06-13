import { createQuery } from "@askrjs/askr/data";
import { sessionService } from "./session-service";
import type { ActiveSessionsOverview, SessionState } from "./session-models";

const CURRENT_SESSION_KEY = "session:current";
const ACTIVE_SESSIONS_KEY = "session:active";

async function fetchCurrentSession({ signal }: { signal: AbortSignal }) {
  return (
    (await sessionService.getCurrentSession({ signal })) ?? {
      authenticated: false,
      username: "admin",
    }
  );
}

function fetchActiveSessions({ signal }: { signal: AbortSignal }) {
  return sessionService.listActiveSessions({ signal });
}

export function createCurrentSessionQuery() {
  return createQuery<SessionState>({
    key: CURRENT_SESSION_KEY,
    fetch: fetchCurrentSession,
  });
}

export function createActiveSessionsQuery() {
  return createQuery<ActiveSessionsOverview>({
    key: ACTIVE_SESSIONS_KEY,
    fetch: fetchActiveSessions,
  });
}
