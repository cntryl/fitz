export type AdminSearchDomain =
  | "sessions"
  | "kv"
  | "stream"
  | "queue"
  | "schedule"
  | "lease"
  | "notice"
  | "rpc";

export interface AdminSearchRequest {
  area?: string;
  domain?: AdminSearchDomain;
  limit?: number;
  operation?: string;
  query: string;
  realm?: string;
  resource?: string;
  routeFamily?: string;
}

export interface AdminSearchResult {
  area?: string;
  domain: string;
  health?: string;
  href: string;
  id: string;
  kind: string;
  matchedFields: string[];
  metadata: Record<string, string>;
  operation?: string;
  realm?: string;
  resource?: string;
  routeFamily?: string;
  summary: string;
  title: string;
}

export interface AdminSearchResults {
  domain?: string;
  limit: number;
  query: string;
  results: AdminSearchResult[];
  routeFamily?: string;
  total: number;
  truncated: boolean;
}
