import { FetchClient, addProductionStack } from "@fgrzl/fetch";
import { appConfig } from "@/shared/config";

function createTraceId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }

  return `fitz-ui-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

// Adapter boundary only: configure transport concerns here, not DTO mapping or app logic.
export const client = addProductionStack(
  new FetchClient({
    baseUrl: appConfig.apiBaseUrl,
    credentials: "same-origin",
    timeout: appConfig.requestTimeoutMs,
  }),
  {
    retry: {
      maxRetries: 2,
      delay: 750,
    },
    rateLimit: {
      maxRequests: 100,
      windowMs: 60 * 1000,
    },
    logging: {
      level: appConfig.logLevel,
      skipPatterns: ["/metrics"],
    },
  },
);

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
