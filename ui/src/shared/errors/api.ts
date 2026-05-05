import type { FetchResponse } from "@/adapters/generated/types";

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

function errorMessage<T>(response: FetchResponse<T>, fallbackMessage: string) {
  return response.error?.message || response.statusText || fallbackMessage;
}

// Service boundary helper: unwrap FetchResponse<T> before data leaves services.
export function unwrapResponse<T>(response: FetchResponse<T>, fallbackMessage: string): T {
  if (response.ok && response.data !== null) {
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

export function ensureResponseOk<T>(response: FetchResponse<T>, fallbackMessage: string) {
  if (response.ok) {
    return;
  }

  throw new AppApiError(
    errorMessage(response, fallbackMessage),
    response.status,
    errorCodeForStatus(response.status),
  );
}
