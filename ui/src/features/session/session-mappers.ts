import type { LoginRequest, SessionInfo, SessionResponse } from "@/adapters/generated/types";
import type {
  ActiveSession,
  ActiveSessionsOverview,
  LoginPayload,
  SessionState,
} from "./session-models";

// Explicit mapper boundary: snake_case DTO fields stop here.
export function mapSessionResponse(dto: SessionResponse): SessionState {
  return {
    authenticated: dto.authenticated,
    username: dto.username,
  };
}

export function mapLoginPayload(payload: LoginPayload): LoginRequest {
  return {
    username: payload.username,
    password: payload.password,
  };
}

export function mapActiveSession(dto: SessionInfo): ActiveSession {
  const key = [
    dto.session_id,
    dto.route_family,
    dto.identity_value,
    dto.remote_addr,
    dto.connected_at,
  ]
    .filter((value) => value != null && value !== "")
    .join(":");

  return {
    key: key || "session",
    connectedAt: dto.connected_at,
    idleSeconds: dto.idle_seconds,
    identityClaim: dto.identity_claim,
    identityValue: dto.identity_value,
    messagesReceived: dto.messages_received,
    messagesSent: dto.messages_sent,
    remoteAddress: dto.remote_addr,
    routeFamily: dto.route_family,
    sessionId: dto.session_id,
    subject: dto.subject,
    transport: dto.transport,
  };
}

export function mapActiveSessionsOverview(sessions: SessionInfo[]): ActiveSessionsOverview {
  return {
    sessions: sessions.map(mapActiveSession),
  };
}
