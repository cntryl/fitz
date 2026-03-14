export interface SessionState {
  authenticated: boolean;
  username: string;
}

export interface LoginPayload {
  username: string;
  password: string;
}

export async function fetchSession(): Promise<SessionState | null> {
  const response = await fetch("/api/v1/session", {
    credentials: "include",
  });

  if (response.status === 401) {
    return null;
  }

  if (!response.ok) {
    throw new Error("Unable to load admin session");
  }

  return (await response.json()) as SessionState;
}

export async function createSession(payload: LoginPayload): Promise<void> {
  const response = await fetch("/api/v1/session", {
    method: "POST",
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (response.status === 401) {
    throw new Error("Invalid username or password");
  }

  if (!response.ok) {
    throw new Error("Unable to sign in");
  }
}

export async function deleteSession(): Promise<void> {
  const response = await fetch("/api/v1/session", {
    method: "DELETE",
    credentials: "include",
  });

  if (!response.ok) {
    throw new Error("Unable to sign out");
  }
}
