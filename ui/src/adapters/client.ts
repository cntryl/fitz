import { FetchClient } from "@fgrzl/fetch";
import { addLogging, addRateLimit, createRetryMiddleware } from "@fgrzl/fetch/middleware";
import { navigate } from "@askrjs/askr/router";
import { appConfig } from "@/shared/config";

function createTraceId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }

  return `fitz-ui-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

// Adapter boundary only: configure transport concerns here, not DTO mapping or app logic.
export const client = new FetchClient({
  credentials: "same-origin",
  timeout: appConfig.requestTimeoutMs,
});

const retry = createRetryMiddleware({
  maxRetries: 2,
  delay: 750,
});

client.use((request, next) => {
  if (request.method === "GET" || request.method === "HEAD") {
    return retry(request, next);
  }

  return next(request);
});

client.use(async (request, next) => {
  const response = await next(request);
  if (
    response.status === 401 &&
    typeof window !== "undefined" &&
    request.method !== "DELETE" &&
    !window.location.pathname.startsWith("/login")
  ) {
    void fetch("/api/v1/session", {
      credentials: "same-origin",
      headers: { Accept: "application/json" },
      method: "DELETE",
    });
    navigate("/login", { history: "replace" });
  }
  return response;
});

addRateLimit(client, {
  maxRequests: 100,
  windowMs: 60 * 1000,
});

addLogging(client, {
  level: appConfig.logLevel,
});

client.use((request, next) => {
  const headers = new Headers(request.headers);

  if (!headers.has("Accept")) {
    headers.set("Accept", "application/json");
  }

  if (!headers.has("x-request-id")) {
    headers.set("x-request-id", createTraceId());
  }

  return next({
    ...request,
    headers,
  });
});
