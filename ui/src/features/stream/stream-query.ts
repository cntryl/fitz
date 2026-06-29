import { createQuery, queryScope } from "@askrjs/askr/data";
import { stableQueryFetch, type QueryFetch } from "@/shared/query-fetch";
import { streamService } from "./stream-service";
import type {
  StreamAreaRollup,
  StreamOverview,
  StreamRealmRollup,
  StreamResourceView,
} from "./stream-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const streamQueries = queryScope("stream");
const streamRealmFetches = new Map<string, QueryFetch<StreamRealmRollup>>();
const streamAreaFetches = new Map<string, QueryFetch<StreamAreaRollup>>();
const streamResourceFetches = new Map<string, QueryFetch<StreamResourceView>>();

export function streamRealmQueryKey(realm: string, family = currentRouteFamilySegment()) {
  return streamQueries.key("realm", family, realm);
}

export function streamAreaQueryKey(
  realm: string,
  area: string,
  family = currentRouteFamilySegment(),
) {
  return streamQueries.key("area", family, realm, area);
}

export function streamResourceQueryKey(
  request: {
    area: string;
    discriminator?: string;
    fromOffset?: number;
    limit?: number;
    realm: string;
    resource: string;
  },
  family = currentRouteFamilySegment(),
) {
  return streamQueries.key(
    "resource",
    family,
    request.realm,
    request.area,
    request.resource,
    String(request.fromOffset ?? 0),
    request.discriminator ?? "",
    String(request.limit ?? 50),
  );
}

export function createStreamOverviewQuery() {
  const key = streamQueries.key("overview", currentRouteFamilySegment());

  return createQuery<StreamOverview>({
    key,
    fetch: streamService.getOverview,
  });
}

export function createStreamRealmQuery(realm: string) {
  const key = streamRealmQueryKey(realm);

  return createQuery<StreamRealmRollup>({
    key,
    fetch: stableQueryFetch(
      streamRealmFetches,
      key,
      () =>
        ({ signal }) =>
          streamService.getRealmRollup(realm, { signal }),
    ),
  });
}

export function createStreamAreaQuery(realm: string, area: string) {
  const key = streamAreaQueryKey(realm, area);

  return createQuery<StreamAreaRollup>({
    key,
    fetch: stableQueryFetch(
      streamAreaFetches,
      key,
      () =>
        ({ signal }) =>
          streamService.getAreaRollup(realm, area, { signal }),
    ),
  });
}

export function createStreamResourceQuery(request: {
  area: string;
  discriminator?: string;
  fromOffset?: number;
  limit?: number;
  realm: string;
  resource: string;
}) {
  const limit = request.limit ?? 50;
  const key = streamResourceQueryKey({ ...request, limit });

  return createQuery<StreamResourceView>({
    key,
    fetch: stableQueryFetch(
      streamResourceFetches,
      key,
      () =>
        ({ signal }) =>
          streamService.getResourceView(
            {
              ...request,
              limit,
              routeFamily: currentRouteFamilySegment(),
            },
            { signal },
          ),
    ),
  });
}
