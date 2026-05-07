export interface SessionState {
  authenticated: boolean;
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
  messagesReceived?: number;
  messagesSent?: number;
  realm?: string;
  remoteAddress?: string;
  sessionId?: string;
  transport?: string;
}

export interface ActiveSessionsOverview {
  realm?: string;
  sessions: ActiveSession[];
}
