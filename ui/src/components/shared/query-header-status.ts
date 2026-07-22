import type { DomainHeaderProps } from "./domain-header";

type QueryHeaderStatus = NonNullable<DomainHeaderProps["status"]>;

export interface QueryPresentationState {
  data?: unknown;
  error?: unknown;
  loading?: boolean;
  refreshing?: boolean;
  stale?: boolean;
}

export interface QueryHeaderStatusCopy {
  loading: string;
  ready: string;
  unavailable: string;
}

export interface QueryReadyStatus {
  label?: string;
  tone?: QueryHeaderStatus["tone"];
}

function hasQueryData(query: QueryPresentationState) {
  return query.data !== null && query.data !== undefined;
}

export function queryHeaderStatus(
  query: QueryPresentationState,
  copy: QueryHeaderStatusCopy,
  ready: QueryReadyStatus = {},
): QueryHeaderStatus {
  const hasData = hasQueryData(query);

  if (query.refreshing) {
    return {
      detail: hasData ? copy.ready : copy.loading,
      label: "Refreshing",
      tone: "info",
    };
  }

  if (query.error) {
    return {
      detail: copy.unavailable,
      label: hasData ? "Update unavailable" : "Unavailable",
      tone: "warning",
    };
  }

  if (query.stale) {
    return {
      detail: hasData ? copy.ready : copy.unavailable,
      label: "Stale",
      tone: "warning",
    };
  }

  if (query.loading || !hasData) {
    return {
      detail: copy.loading,
      label: "Loading",
      tone: "info",
    };
  }

  return {
    detail: copy.ready,
    label: ready.label ?? "Live",
    tone: ready.tone ?? "success",
  };
}

export function queryFreshness(query: QueryPresentationState) {
  const hasData = hasQueryData(query);

  if (query.refreshing) return "Refreshing";
  if (query.error) return hasData ? "Stale" : "Unavailable";
  if (query.stale) return "Stale";
  if (query.loading || !hasData) return "Loading";
  return "Live";
}
