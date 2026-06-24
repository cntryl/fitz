import { createQuery, queryScope } from "@askrjs/askr/data";
import { searchService } from "./search-service";
import type { AdminSearchRequest, AdminSearchResults } from "./search-models";
import { stableQueryFetch, type QueryFetch } from "@/shared/query-fetch";

const searchQueries = queryScope("search");
const adminSearchFetches = new Map<string, QueryFetch<AdminSearchResults>>();

export function createAdminSearchQuery(request: AdminSearchRequest) {
  const key = searchQueries.key(
    "admin",
    request.query,
    request.routeFamily ?? "all",
    request.domain ?? "any",
    request.realm ?? "any",
    request.area ?? "any",
    request.resource ?? "any",
    request.operation ?? "any",
    request.limit ?? 50,
  );

  return createQuery<AdminSearchResults>({
    key,
    fetch: stableQueryFetch(adminSearchFetches, key, () => ({ signal }) =>
      searchService.searchAdminState(request, { signal }),
    ),
  });
}

export type { AdminSearchRequest, AdminSearchResults } from "./search-models";
