import type { AdminSearchResponse, AdminSearchResult as AdminSearchResultDto } from "@/adapters";
import type { AdminSearchResult, AdminSearchResults } from "./search-models";

function optionalString(value: string | null | undefined) {
  return value ?? undefined;
}

export function mapAdminSearchResult(dto: AdminSearchResultDto): AdminSearchResult {
  return {
    area: optionalString(dto.area),
    domain: dto.domain,
    health: optionalString(dto.health),
    href: dto.href,
    id: dto.id,
    kind: dto.kind,
    matchedFields: dto.matched_fields,
    metadata: dto.metadata,
    operation: optionalString(dto.operation),
    realm: optionalString(dto.realm),
    resource: optionalString(dto.resource),
    routeFamily: optionalString(dto.route_family),
    summary: dto.summary,
    title: dto.title,
  };
}

export function mapAdminSearchResponse(dto: AdminSearchResponse): AdminSearchResults {
  return {
    domain: optionalString(dto.domain),
    limit: dto.limit,
    query: dto.query,
    results: dto.results.map(mapAdminSearchResult),
    routeFamily: optionalString(dto.route_family),
    total: dto.total,
    truncated: dto.truncated,
  };
}
