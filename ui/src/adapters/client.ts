import { FetchClient, addProductionStack } from "@fgrzl/fetch";

const API_BASE_URL = import.meta.env.VITE_FITZ_API_BASE_URL ?? "";
const REQUEST_TIMEOUT_MS = 10_000;

function createTraceId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }

  return `fitz-ui-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

// Adapter boundary only: configure transport concerns here, not DTO mapping or app logic.
export const client = addProductionStack(
  new FetchClient({
    baseUrl: API_BASE_URL,
    credentials: "same-origin",
    timeout: REQUEST_TIMEOUT_MS,
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
      level: import.meta.env.DEV ? "debug" : "warn",
      skipPatterns: ["/health", "/healthz", "/readyz", "/startupz", "/metrics"],
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
