type FitzLogLevel = "debug" | "info" | "warn" | "error";

const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const DEFAULT_DASHBOARD_POLL_INTERVAL_MS = 10_000;

function parseLogLevel(value: string | undefined): FitzLogLevel {
  if (value === "debug" || value === "info" || value === "warn" || value === "error") {
    return value;
  }

  if (value) {
    throw new Error(
      `Invalid VITE_FITZ_LOG_LEVEL value "${value}". Expected debug, info, warn, or error.`,
    );
  }

  return import.meta.env.DEV ? "debug" : "warn";
}

function parseTimeoutMs(value: string | undefined) {
  if (!value) {
    return DEFAULT_REQUEST_TIMEOUT_MS;
  }

  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(
      `Invalid VITE_FITZ_REQUEST_TIMEOUT_MS value "${value}". Expected a positive number of milliseconds.`,
    );
  }

  return parsed;
}

export function parseDashboardPollIntervalMs(value: string | undefined) {
  if (!value) {
    return DEFAULT_DASHBOARD_POLL_INTERVAL_MS;
  }

  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(
      `Invalid VITE_FITZ_DASHBOARD_POLL_INTERVAL_MS value "${value}". Expected a positive number of milliseconds.`,
    );
  }

  return parsed;
}

export const appConfig = {
  dashboardPollIntervalMs: parseDashboardPollIntervalMs(
    import.meta.env.VITE_FITZ_DASHBOARD_POLL_INTERVAL_MS,
  ),
  environmentLabel: import.meta.env.VITE_FITZ_ENVIRONMENT_LABEL ?? "Local",
  logLevel: parseLogLevel(import.meta.env.VITE_FITZ_LOG_LEVEL),
  requestTimeoutMs: parseTimeoutMs(import.meta.env.VITE_FITZ_REQUEST_TIMEOUT_MS),
} as const;

export type AppConfig = typeof appConfig;
