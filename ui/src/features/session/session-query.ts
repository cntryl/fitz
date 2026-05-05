import { createQuery, type Query } from "@/shared/query/query";
import { sessionService } from "./session-service";
import type { SessionState } from "./session-models";

const CURRENT_SESSION_KEY = "session:current";

export interface CurrentSessionQuery extends Query<SessionState | null> {
  key: string;
}

export function createCurrentSessionQuery(): CurrentSessionQuery {
  const query = createQuery({
    key: CURRENT_SESSION_KEY,
    fetch: ({ signal }) => sessionService.getCurrentSession({ signal }),
  });

  return Object.assign(query, {
    key: CURRENT_SESSION_KEY,
  });
}
