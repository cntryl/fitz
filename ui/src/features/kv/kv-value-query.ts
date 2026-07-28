import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { kvService } from "./kv-service";
import type { KvCommittedResourceScope, KvCommittedValueResult, KvKeyEncoding } from "./kv-models";

const kvValueQueries = queryScope("kv");

interface KvValueQueryInput {
  key: string;
  keyEncoding: KvKeyEncoding;
  scope: KvCommittedResourceScope;
}

export function kvValueQueryKey(
  scope: KvCommittedResourceScope,
  key: string,
  keyEncoding: KvKeyEncoding,
) {
  return kvValueQueries.key(
    "value",
    scope.routeFamily,
    scope.realm,
    scope.area,
    scope.resource,
    keyEncoding,
    key,
  );
}

const kvValueQuery = defineQuery<KvValueQueryInput, KvCommittedValueResult>({
  key: ({ key, keyEncoding, scope }) => kvValueQueryKey(scope, key, keyEncoding),
  fetch: ({ key, keyEncoding, scope, signal }) =>
    kvService.getCommittedValue(scope, key, keyEncoding, { signal }),
});

export function createKvValueQuery(
  scope: KvCommittedResourceScope,
  key: string,
  keyEncoding: KvKeyEncoding,
  options?: { skipInitialFetch?: boolean },
) {
  return createQuery(kvValueQuery, { key, keyEncoding, scope }, options);
}
