import type { FetchResult } from "@askrjs/fetch";

export type AppApiErrorCode =
  | "badRequest"
  | "unauthenticated"
  | "forbidden"
  | "notFound"
  | "conflict"
  | "serviceUnavailable"
  | "emptyResponse"
  | "unknown";

export interface ServiceRequestOptions {
  signal?: AbortSignal;
  timeout?: number;
}

export class AppApiError extends Error {
  readonly code: AppApiErrorCode;
  readonly status: number;

  constructor(message: string, status: number, code: AppApiErrorCode) {
    super(message);
    this.name = "AppApiError";
    this.status = status;
    this.code = code;
  }
}

function errorCodeForStatus(status: number): AppApiErrorCode {
  if (status === 400) return "badRequest";
  if (status === 401) return "unauthenticated";
  if (status === 403) return "forbidden";
  if (status === 404) return "notFound";
  if (status === 409) return "conflict";
  if (status === 503) return "serviceUnavailable";
  return "unknown";
}

function errorMessage<T>(response: FetchResult<T>, fallbackMessage: string) {
  if (response.ok) return fallbackMessage;
  if (
    typeof response.error === "object" &&
    response.error !== null &&
    "message" in response.error
  ) {
    return String(response.error.message);
  }
  return fallbackMessage;
}

// Service boundary helper: unwrap FetchResult<T> before data leaves services.
export function unwrapResponse<T>(response: FetchResult<T>, fallbackMessage: string): T {
  if (response.ok && response.data !== null && response.data !== undefined) {
    return response.data;
  }

  if (response.ok) {
    throw new AppApiError(fallbackMessage, response.status, "emptyResponse");
  }

  throw new AppApiError(
    errorMessage(response, fallbackMessage),
    response.status,
    errorCodeForStatus(response.status),
  );
}

export function ensureResponseOk<T>(response: FetchResult<T>, fallbackMessage: string) {
  if (response.ok) {
    return;
  }

  throw new AppApiError(
    errorMessage(response, fallbackMessage),
    response.status,
    errorCodeForStatus(response.status),
  );
}
