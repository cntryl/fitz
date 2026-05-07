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
  const key = [dto.session_id, dto.realm, dto.remote_addr, dto.connected_at]
    .filter((value) => value != null && value !== "")
    .join(":");

  return {
    key: key || "session",
    connectedAt: dto.connected_at,
    idleSeconds: dto.idle_seconds,
    messagesReceived: dto.messages_received,
    messagesSent: dto.messages_sent,
    realm: dto.realm,
    remoteAddress: dto.remote_addr,
    sessionId: dto.session_id,
    transport: dto.transport,
  };
}

export function mapActiveSessionsOverview(
  realm: string | undefined,
  sessions: SessionInfo[],
): ActiveSessionsOverview {
  return {
    realm,
    sessions: sessions.map(mapActiveSession),
  };
}
