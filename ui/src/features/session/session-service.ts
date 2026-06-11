import { apiv1 } from "@/adapters";
import { appConfig } from "@/shared/config";
import {
  AppApiError,
  ensureResponseOk,
  unwrapResponse,
  type ServiceRequestOptions,
} from "@/shared/errors/api";
import { mapActiveSessionsOverview, mapLoginPayload, mapSessionResponse } from "./session-mappers";
import type { ActiveSessionsOverview, LoginPayload, SessionState } from "./session-models";

export type { LoginPayload, SessionState } from "./session-models";

interface AdminFeatures {
  admin_auth_required: boolean;
  admin_auth_mode: "protected" | "open";
}

const OPEN_ADMIN_SESSION: SessionState = {
  authenticated: true,
  username: "admin",
};

function featuresUrl() {
  const baseUrl = appConfig.apiBaseUrl.replace(/\/$/, "");
  return `${baseUrl}/api/v1/features`;
}

async function getAdminFeatures(options: ServiceRequestOptions = {}): Promise<AdminFeatures> {
  const response = await fetch(featuresUrl(), {
    credentials: "same-origin",
    headers: {
      Accept: "application/json",
    },
    signal: options.signal,
  });

  if (!response.ok) {
    throw new AppApiError("Unable to load admin features", response.status, "unknown");
  }

  return (await response.json()) as AdminFeatures;
}

async function getCurrentSession(
  options: ServiceRequestOptions = {},
): Promise<SessionState | null> {
  const features = await getAdminFeatures(options);

  if (!features.admin_auth_required) {
    return OPEN_ADMIN_SESSION;
  }

  const response = await apiv1.getAdminSession(options);

  if (response.status === 401) {
    return null;
  }

  return mapSessionResponse(unwrapResponse(response, "Unable to load admin session"));
}

async function signIn(payload: LoginPayload, options: ServiceRequestOptions = {}): Promise<void> {
  const features = await getAdminFeatures(options);

  if (!features.admin_auth_required) {
    return;
  }

  const response = await apiv1.createAdminSession(mapLoginPayload(payload), options);

  if (response.status === 401) {
    throw new AppApiError("Invalid username or password", 401, "unauthenticated");
  }

  ensureResponseOk(response, "Unable to sign in");
}

async function signOut(options: ServiceRequestOptions = {}): Promise<void> {
  const features = await getAdminFeatures(options);

  if (!features.admin_auth_required) {
    return;
  }

  const response = await apiv1.deleteAdminSession(options);

  if (response.status === 401) {
    return;
  }

  ensureResponseOk(response, "Unable to sign out");
}

async function listActiveSessions(
  options: ServiceRequestOptions = {},
): Promise<ActiveSessionsOverview> {
  const response = await apiv1.listActiveSessions(options);

  return mapActiveSessionsOverview(
    unwrapResponse(response, "Unable to load active sessions").sessions,
  );
}

// Services own app-facing method names and return plain promises/models.
export const sessionService = {
  getAdminFeatures,
  getCurrentSession,
  listActiveSessions,
  signIn,
  signOut,
};
