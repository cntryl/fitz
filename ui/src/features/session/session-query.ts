import { createQuery, queryScope } from "@askrjs/askr/data";
import { sessionService } from "./session-service";
import type { ActiveSessionsOverview, SessionState } from "./session-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const sessionQueries = queryScope("session");

export const SESSION_QUERY_PREFIX = sessionQueries.prefix();

const CURRENT_SESSION_KEY = sessionQueries.key("current");
export function activeSessionsQueryKey(family = currentRouteFamilySegment()) {
  return sessionQueries.key("active", family);
}

async function fetchCurrentSession({ signal }: { signal: AbortSignal }) {
  return (
    (await sessionService.getCurrentSession({ signal })) ?? {
      authenticated: false,
      routeFamilies: [],
      routeFamiliesWildcard: true,
      username: "admin",
    }
  );
}

function fetchActiveSessions(family: string) {
  return ({ signal }: { signal: AbortSignal }) =>
    sessionService.listActiveSessions(family, { signal });
}

export function createCurrentSessionQuery() {
  return createQuery<SessionState>({
    key: CURRENT_SESSION_KEY,
    fetch: fetchCurrentSession,
  });
}

export function createActiveSessionsQuery(family = currentRouteFamilySegment()) {
  return createQuery<ActiveSessionsOverview>({
    key: activeSessionsQueryKey(family),
    fetch: fetchActiveSessions(family),
  });
}
