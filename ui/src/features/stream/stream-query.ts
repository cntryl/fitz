import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { streamService } from "./stream-service";
import type {
  StreamAreaRollup,
  StreamOverview,
  StreamRealmRollup,
  StreamResourceView,
} from "./stream-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const streamQueries = queryScope("stream");

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

const streamOverviewQuery = defineQuery<{ family: string }, StreamOverview>({
  key: ({ family }) => streamQueries.key("overview", family),
  fetch: ({ signal }) => streamService.getOverview({ signal }),
});

const streamRealmQuery = defineQuery<{ family: string; realm: string }, StreamRealmRollup>({
  key: ({ family, realm }) => streamRealmQueryKey(realm, family),
  fetch: ({ realm, signal }) => streamService.getRealmRollup(realm, { signal }),
});

const streamAreaQuery = defineQuery<
  { area: string; family: string; realm: string },
  StreamAreaRollup
>({
  key: ({ area, family, realm }) => streamAreaQueryKey(realm, area, family),
  fetch: ({ area, realm, signal }) => streamService.getAreaRollup(realm, area, { signal }),
});

interface StreamResourceQueryInput {
  area: string;
  discriminator?: string;
  family: string;
  fromOffset?: number;
  limit: number;
  realm: string;
  resource: string;
}

const streamResourceQuery = defineQuery<StreamResourceQueryInput, StreamResourceView>({
  key: ({ family, ...request }) => streamResourceQueryKey(request, family),
  fetch: ({ family, signal, ...request }) =>
    streamService.getResourceView({ ...request, routeFamily: family }, { signal }),
});

export function createStreamOverviewQuery() {
  return createQuery(streamOverviewQuery, { family: currentRouteFamilySegment() });
}

export function createStreamRealmQuery(realm: string) {
  return createQuery(streamRealmQuery, { family: currentRouteFamilySegment(), realm });
}

export function createStreamAreaQuery(realm: string, area: string) {
  return createQuery(streamAreaQuery, { area, family: currentRouteFamilySegment(), realm });
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
  return createQuery(streamResourceQuery, {
    ...request,
    family: currentRouteFamilySegment(),
    limit,
  });
}
