import { FetchClient } from "@fgrzl/fetch";
import { addLogging, addRateLimit, addRetry } from "@fgrzl/fetch/middleware";
import { appConfig } from "@/shared/config";

const DOMAIN_API_SEGMENTS = new Set([
  "kv",
  "queue",
  "stream",
  "lease",
  "schedule",
  "notice",
  "rpc",
]);
const ROUTE_FAMILY_STORAGE_KEY = "fitz-admin-route-family";
const DEFAULT_ROUTE_FAMILY_SEGMENT = "1";
const routeFamilyPattern = /^\d+$/;

function createTraceId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }

  return `fitz-ui-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function routeFamilyFromLocation() {
  if (typeof window === "undefined") {
    return DEFAULT_ROUTE_FAMILY_SEGMENT;
  }

  const parts = window.location.pathname.split("/").filter(Boolean);
  if (parts[0] === "admin" && routeFamilyPattern.test(parts[1] ?? "")) {
    return decodeURIComponent(parts[1]);
  }

  const storedRouteFamily = window.localStorage?.getItem(ROUTE_FAMILY_STORAGE_KEY);

  return routeFamilyPattern.test(storedRouteFamily ?? "")
    ? (storedRouteFamily ?? DEFAULT_ROUTE_FAMILY_SEGMENT)
    : DEFAULT_ROUTE_FAMILY_SEGMENT;
}

function familyFirstAdminUrl(url: string | undefined) {
  if (!url) {
    return url;
  }

  const parsed = new URL(url, "http://fitz.local");
  const parts = parsed.pathname.split("/").filter(Boolean);

  if (parts[0] !== "api" || parts[1] !== "v1" || !DOMAIN_API_SEGMENTS.has(parts[2])) {
    return url;
  }

  parts.splice(2, 0, routeFamilyFromLocation());
  parsed.pathname = `/${parts.map((part) => encodeURIComponent(decodeURIComponent(part))).join("/")}`;

  if (/^https?:\/\//.test(url)) {
    return parsed.toString();
  }

  return `${parsed.pathname}${parsed.search}${parsed.hash}`;
}

// Adapter boundary only: configure transport concerns here, not DTO mapping or app logic.
export const client = new FetchClient({
  baseUrl: appConfig.apiBaseUrl,
  credentials: "same-origin",
  timeout: appConfig.requestTimeoutMs,
});

client.use((request, next) => {
  return next({
    ...request,
    url: familyFirstAdminUrl(request.url),
  });
});

addRetry(client, {
  maxRetries: 2,
  delay: 750,
});

addRateLimit(client, {
  maxRequests: 100,
  windowMs: 60 * 1000,
  skipPatterns: ["/metrics"],
});

addLogging(client, {
  level: appConfig.logLevel,
  skipPatterns: ["/metrics"],
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
