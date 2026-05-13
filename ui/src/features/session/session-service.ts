import { apiv1 } from "@/adapters";
import {
  AppApiError,
  ensureResponseOk,
  unwrapResponse,
  type ServiceRequestOptions,
} from "@/shared/errors/api";
import { mapActiveSessionsOverview, mapLoginPayload, mapSessionResponse } from "./session-mappers";
import type { ActiveSessionsOverview, LoginPayload, SessionState } from "./session-models";

export type { LoginPayload, SessionState } from "./session-models";

async function getCurrentSession(
  options: ServiceRequestOptions = {},
): Promise<SessionState | null> {
  const response = await apiv1.getAdminSession(options);

  if (response.status === 401) {
    return null;
  }

  return mapSessionResponse(unwrapResponse(response, "Unable to load admin session"));
}

async function signIn(payload: LoginPayload, options: ServiceRequestOptions = {}): Promise<void> {
  const response = await apiv1.createAdminSession(mapLoginPayload(payload), options);

  if (response.status === 401) {
    throw new AppApiError("Invalid username or password", 401, "unauthenticated");
  }

  ensureResponseOk(response, "Unable to sign in");
}

async function signOut(options: ServiceRequestOptions = {}): Promise<void> {
  ensureResponseOk(await apiv1.deleteAdminSession(options), "Unable to sign out");
}

async function listActiveSessions(
  realm: string | undefined = undefined,
  options: ServiceRequestOptions = {},
): Promise<ActiveSessionsOverview> {
  const response = await apiv1.listActiveSessions(realm ? { realm } : undefined, options);

  return mapActiveSessionsOverview(
    realm,
    unwrapResponse(response, "Unable to load active sessions").sessions,
  );
}

// Services own app-facing method names and return plain promises/models.
export const sessionService = {
  getCurrentSession,
  listActiveSessions,
  signIn,
  signOut,
};
