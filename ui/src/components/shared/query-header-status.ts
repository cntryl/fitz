import type { DomainHeaderProps } from "./domain-header";

type QueryHeaderStatus = NonNullable<DomainHeaderProps["status"]>;

export interface QueryPresentationState {
  data?: unknown;
  error?: unknown;
  loading?: boolean;
  refreshing?: boolean;
  stale?: boolean;
}

export interface QueryReadyStatus {
  label?: string;
  tone?: QueryHeaderStatus["tone"];
}

type QueryFreshnessStatus = NonNullable<QueryHeaderStatus["freshness"]>;

function queryFreshnessBadge(query: QueryPresentationState): QueryFreshnessStatus | undefined {
  if (query.refreshing) return { label: "Refreshing", tone: "info" };
  if (query.error) return { label: "Update unavailable", tone: "warning" };
  if (query.stale) return { label: "Stale", tone: "warning" };
  if (query.loading) return { label: "Updating", tone: "info" };
  return undefined;
}

function hasQueryData(query: QueryPresentationState) {
  return query.data !== null && query.data !== undefined;
}

export function queryHeaderStatus(
  query: QueryPresentationState,
  ready: QueryReadyStatus = {},
): QueryHeaderStatus | undefined {
  const hasData = hasQueryData(query);

  if (hasData) {
    const freshness = queryFreshnessBadge(query);

    if (ready.label || freshness) {
      return {
        ...(ready.label ? { label: ready.label, tone: ready.tone ?? "success" } : {}),
        ...(freshness ? { freshness } : {}),
      };
    }

    return undefined;
  }

  if (query.loading || query.refreshing) {
    return {
      label: "Loading",
      tone: "info",
    };
  }

  if (query.error) return { label: "Unavailable", tone: "warning" };
  return undefined;
}

export function queryFreshness(query: QueryPresentationState) {
  const hasData = hasQueryData(query);

  if (query.refreshing) return "Refreshing";
  if (query.error) return hasData ? "Stale" : "Unavailable";
  if (query.stale) return "Stale";
  if (query.loading || !hasData) return "Loading";
  return "Live";
}
