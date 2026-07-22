import { createQuery, defineQuery, queryScope } from "@askrjs/askr/data";
import { topologyService } from "./topology-service";
import type { MessagingTopologyOverview } from "./topology-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const topologyQueries = queryScope("topology");

export const MESSAGING_TOPOLOGY_KEY = topologyQueries.key("overview");

export function messagingTopologyQueryKey(family = currentRouteFamilySegment()) {
  return topologyQueries.key("overview", family);
}

const messagingTopologyQuery = defineQuery<{ family: string }, MessagingTopologyOverview>({
  key: ({ family }) => messagingTopologyQueryKey(family),
  fetch: ({ family, signal }) => topologyService.getOverview(family, { signal }),
});

export function createMessagingTopologyQuery(family = currentRouteFamilySegment()) {
  return createQuery(messagingTopologyQuery, { family });
}
