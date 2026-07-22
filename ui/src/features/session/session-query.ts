import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
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
      authRequired: true,
      authenticated: false,
      routeFamilies: [],
      routeFamiliesWildcard: true,
      username: "",
    }
  );
}

const currentSessionQuery = defineQuery<Record<never, never>, SessionState>({
  key: () => CURRENT_SESSION_KEY,
  fetch: fetchCurrentSession,
});

const activeSessionsQuery = defineQuery<{ family: string }, ActiveSessionsOverview>({
  key: ({ family }) => activeSessionsQueryKey(family),
  fetch: ({ family, signal }) => sessionService.listActiveSessions(family, { signal }),
});

export function createCurrentSessionQuery() {
  return createQuery(currentSessionQuery, {});
}

export function createActiveSessionsQuery(family = currentRouteFamilySegment()) {
  return createQuery(activeSessionsQuery, { family });
}
