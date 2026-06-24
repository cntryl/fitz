import { createQuery, queryScope } from "@askrjs/askr/data";
import { searchService } from "./search-service";
import type { AdminSearchRequest, AdminSearchResults } from "./search-models";

const searchQueries = queryScope("search");

export function createAdminSearchQuery(request: AdminSearchRequest) {
  return createQuery<AdminSearchResults>({
    key: searchQueries.key(
      "admin",
      request.query,
      request.routeFamily ?? "all",
      request.domain ?? "any",
      request.realm ?? "any",
      request.area ?? "any",
      request.resource ?? "any",
      request.operation ?? "any",
      request.limit ?? 50,
    ),
    fetch: ({ signal }) => searchService.searchAdminState(request, { signal }),
  });
}

export type { AdminSearchRequest, AdminSearchResults } from "./search-models";
