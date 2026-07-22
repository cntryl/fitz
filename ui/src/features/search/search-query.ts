import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { searchService } from "./search-service";
import type { AdminSearchRequest, AdminSearchResults } from "./search-models";

const searchQueries = queryScope("search");
const adminSearchQuery = defineQuery<AdminSearchRequest, AdminSearchResults>({
  key: (request) =>
    searchQueries.key(
      "admin",
      request.query,
      request.routeFamily ?? "unscoped",
      request.domain ?? "any",
      request.realm ?? "any",
      request.area ?? "any",
      request.resource ?? "any",
      request.operation ?? "any",
      request.limit ?? 50,
    ),
  fetch: ({ signal, ...request }) => searchService.searchAdminState(request, { signal }),
});

export function createAdminSearchQuery(request: AdminSearchRequest) {
  return createQuery(adminSearchQuery, request);
}

export type { AdminSearchRequest, AdminSearchResults } from "./search-models";
