import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { kvService } from "./kv-service";
import type { KvOverview, KvResourceDetail, KvResourceScope, KvTransaction } from "./kv-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const kvQueries = queryScope("kv");
const kvOverviewQuery = defineQuery<{ family: string }, KvOverview>({
  key: ({ family }) => kvQueries.key("overview", family),
  fetch: ({ family }, { signal }) => kvService.getOverview({ routeFamily: family, signal }),
});

interface KvResourceQueryInput {
  family: string;
  scope: KvResourceScope;
}

const kvResourceDetailQuery = defineQuery<KvResourceQueryInput, KvResourceDetail>({
  key: ({ family, scope }) =>
    kvQueries.key("resource-detail", family, scope.realm, scope.area, scope.resource),
  fetch: ({ family, scope }, { signal }) =>
    kvService.getResource(scope, { routeFamily: family, signal }),
});

const kvTransactionsQuery = defineQuery<KvResourceQueryInput, KvTransaction[]>({
  key: ({ family, scope }) =>
    kvQueries.key("transactions", family, scope.realm, scope.area, scope.resource),
  fetch: ({ family, scope }, { signal }) =>
    kvService.listTransactions(scope, { routeFamily: family, signal }),
});

export function createKvOverviewQuery() {
  return createQuery(kvOverviewQuery, { family: currentRouteFamilySegment() });
}

export function createKvResourceDetailQuery(scope: KvResourceScope) {
  return createQuery(kvResourceDetailQuery, { family: currentRouteFamilySegment(), scope });
}

export function createKvTransactionsQuery(scope: KvResourceScope) {
  return createQuery(kvTransactionsQuery, { family: currentRouteFamilySegment(), scope });
}
