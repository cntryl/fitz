import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { streamService } from "./stream-service";
import type { StreamAreaRollup, StreamOverview, StreamRealmRollup } from "./stream-models";
import type { StreamRecordsResponse, StreamResourceDetail } from "@/adapters";
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
  fetch: ({ family }, { signal }) => streamService.getOverview({ routeFamily: family, signal }),
});

const streamRealmQuery = defineQuery<{ family: string; realm: string }, StreamRealmRollup>({
  key: ({ family, realm }) => streamRealmQueryKey(realm, family),
  fetch: ({ family, realm }, { signal }) =>
    streamService.getRealmRollup(realm, { routeFamily: family, signal }),
});

const streamAreaQuery = defineQuery<
  { area: string; family: string; realm: string },
  StreamAreaRollup
>({
  key: ({ area, family, realm }) => streamAreaQueryKey(realm, area, family),
  fetch: ({ area, family, realm }, { signal }) =>
    streamService.getAreaRollup(realm, area, { routeFamily: family, signal }),
});

interface StreamRecordsQueryInput {
  area: string;
  discriminator?: string;
  family: string;
  fromOffset?: number;
  limit: number;
  realm: string;
  resource: string;
}

const streamResourceDetailQuery = defineQuery<
  { area: string; family: string; realm: string; resource: string },
  StreamResourceDetail
>({
  key: ({ area, family, realm, resource }) =>
    streamQueries.key("resource-detail", family, realm, area, resource),
  fetch: ({ family, ...scope }, { signal }) =>
    streamService.getResourceDetail({ ...scope, routeFamily: family }, { signal }),
});

const streamRecordsQuery = defineQuery<StreamRecordsQueryInput, StreamRecordsResponse>({
  key: ({ family, ...request }) => streamResourceQueryKey(request, family),
  fetch: ({ family, ...request }, { signal }) =>
    streamService.readResourceRecords({ ...request, routeFamily: family }, { signal }),
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

export function createStreamResourceQuery(scope: {
  area: string;
  realm: string;
  resource: string;
}) {
  return createQuery(streamResourceDetailQuery, { ...scope, family: currentRouteFamilySegment() });
}

/** Records are data rows; create this query only once the operator asks for them. */
export function createStreamRecordsQuery(request: {
  area: string;
  discriminator?: string;
  fromOffset?: number;
  limit?: number;
  realm: string;
  resource: string;
}) {
  return createQuery(streamRecordsQuery, {
    ...request,
    family: currentRouteFamilySegment(),
    limit: request.limit ?? 50,
  });
}
