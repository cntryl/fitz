import { createQuery, queryScope } from "@askrjs/askr/data";
import { kvService } from "./kv-service";
import type { KvResourceScope, KvRowsResult } from "./kv-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";
import { stableQueryFetch, type QueryFetch } from "@/shared/query-fetch";

const kvRowsQueries = queryScope("kv");
const kvRowsFetches = new Map<string, QueryFetch<KvRowsResult>>();

export interface KvRowsQueryState {
  cursor?: string | null;
  limit: number;
  startsWith: string;
}

export function kvRowsQueryKey(scope: KvResourceScope, state: KvRowsQueryState) {
  return kvRowsQueries.key(
    "rows",
    currentRouteFamilySegment(),
    scope.realm,
    scope.area,
    scope.resource,
    state.startsWith,
    state.cursor ?? "start",
    state.limit,
  );
}

export function createKvRowsQuery(scope: KvResourceScope, state: KvRowsQueryState) {
  const key = kvRowsQueryKey(scope, state);

  return createQuery<KvRowsResult>({
    key,
    fetch: stableQueryFetch(
      kvRowsFetches,
      key,
      () =>
        ({ signal }) =>
          kvService.browseCommittedRows(scope, state, { signal }),
    ),
  });
}
