import { resource, type ResourceResult } from "@askrjs/askr/resources";
import { sessionService, type LoginPayload, type SessionState } from "../services/session.service";

const CURRENT_SESSION_KEY = ["currentSession"] as const;
const CURRENT_SESSION_STALE_MS = 5_000;

export interface CurrentSessionQuery extends ResourceResult<SessionState | null> {
  key: typeof CURRENT_SESSION_KEY;
  staleTimeMs: number;
}

export type { LoginPayload, SessionState };

export function createCurrentSessionQuery(): CurrentSessionQuery {
  const result = resource(({ signal }) => sessionService.getCurrentSession({ signal }), [
    ...CURRENT_SESSION_KEY,
  ]);

  return Object.assign(result, {
    key: CURRENT_SESSION_KEY,
    staleTimeMs: CURRENT_SESSION_STALE_MS,
  });
}

export async function signInAdmin(payload: LoginPayload): Promise<void> {
  await sessionService.signIn(payload);
}

export async function signOutAdmin(): Promise<void> {
  await sessionService.signOut();
}
