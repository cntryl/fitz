import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { kvService } from "./kv-service";
import type { KvResourceScope, KvRowsResult } from "./kv-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const kvRowsQueries = queryScope("kv");

export interface KvRowsQueryState {
  cursor?: string | null;
  limit: number;
  startsWith: string;
}

export function kvRowsQueryKey(
  scope: KvResourceScope,
  state: KvRowsQueryState,
  family = currentRouteFamilySegment(),
) {
  return kvRowsQueries.key(
    "rows",
    family,
    scope.realm,
    scope.area,
    scope.resource,
    state.startsWith,
    state.cursor ?? "start",
    state.limit,
  );
}

interface KvRowsQueryInput {
  family: string;
  scope: KvResourceScope;
  state: KvRowsQueryState;
}

const kvRowsQuery = defineQuery<KvRowsQueryInput, KvRowsResult>({
  key: ({ family, scope, state }) => kvRowsQueryKey(scope, state, family),
  fetch: ({ scope, signal, state }) => kvService.browseCommittedRows(scope, state, { signal }),
});

export function createKvRowsQuery(
  scope: KvResourceScope,
  state: KvRowsQueryState,
  options?: { skipInitialFetch?: boolean },
) {
  return createQuery(kvRowsQuery, { family: currentRouteFamilySegment(), scope, state }, options);
}
