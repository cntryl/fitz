export interface SessionState {
  authenticated: boolean;
  routeFamilies: string[];
  routeFamiliesWildcard: boolean;
  username: string;
}

export interface LoginPayload {
  username: string;
  password: string;
}

export interface ActiveSession {
  key: string;
  connectedAt?: string;
  idleSeconds?: number;
  identityClaim?: string;
  identityValue?: string;
  messagesReceived?: number;
  messagesSent?: number;
  remoteAddress?: string;
  routeFamily?: number;
  sessionId?: string;
  subject?: string;
  transport?: string;
}

export interface ActiveSessionsOverview {
  sessions: ActiveSession[];
}
