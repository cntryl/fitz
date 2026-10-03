export interface SessionState {
  authRequired?: boolean;
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
  serviceName?: string;
  sessionId?: string;
  subject?: string;
  transport?: string;
}

export interface ActiveSessionsOverview {
  sessions: ActiveSession[];
}
