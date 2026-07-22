import type { ClientOptions, Middleware } from "@askrjs/fetch";
import { logging, retry } from "@askrjs/fetch/middleware";
import { navigate } from "@askrjs/askr/router";
import { appConfig } from "@/shared/config";

function createTraceId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }

  return `fitz-ui-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

const requestHeaders: Middleware = (context, next) => {
  const headers = new Headers(context.request.headers);

  if (!headers.has("Accept")) headers.set("Accept", "application/json");
  if (!headers.has("x-request-id")) headers.set("x-request-id", createTraceId());

  return next({
    ...context,
    request: new Request(context.request, { headers }),
  });
};

const redirectUnauthenticated: Middleware = async (context, next) => {
  const result = await next(context);

  if (
    result.status === 401 &&
    typeof window !== "undefined" &&
    context.request.method !== "DELETE" &&
    !window.location.pathname.startsWith("/login")
  ) {
    void fetch("/api/v1/session", {
      credentials: "same-origin",
      headers: { Accept: "application/json" },
      method: "DELETE",
    });
    navigate("/login", { history: "replace" });
  }

  return result;
};

const requestLogger = logging({
  log(event) {
    const method = appConfig.logLevel === "debug" ? "debug" : appConfig.logLevel;
    console[method](event);
  },
});

// Adapter boundary only: configure transport concerns here, not DTO mapping or app logic.
export const clientOptions: ClientOptions = {
  credentials: "same-origin",
  timeout: appConfig.requestTimeoutMs,
  middleware: [
    requestHeaders,
    retry({ attempts: 3, delay: () => 750 }),
    redirectUnauthenticated,
    requestLogger,
  ],
};
