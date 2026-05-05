import type { LoginRequest, SessionResponse } from "@/adapters/generated/types";
import type { LoginPayload, SessionState } from "./session-models";

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
