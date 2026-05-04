import type { LoginRequest, SessionResponse } from "../adapters/generated/types";

export interface SessionState {
  authenticated: boolean;
  username: string;
}

export interface LoginPayload {
  username: string;
  password: string;
}

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
