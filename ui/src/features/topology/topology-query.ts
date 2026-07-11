import { createQuery, queryScope } from "@askrjs/askr/data";
import { topologyService } from "./topology-service";
import type { MessagingTopologyOverview } from "./topology-models";
import { currentRouteFamilySegment } from "@/shared/navigation/domains";

const topologyQueries = queryScope("topology");

export const MESSAGING_TOPOLOGY_KEY = topologyQueries.key("overview");

export function messagingTopologyQueryKey(family = currentRouteFamilySegment()) {
  return topologyQueries.key("overview", family);
}

function fetchMessagingTopology(family: string) {
  return ({ signal }: { signal: AbortSignal }) => topologyService.getOverview(family, { signal });
}

export function createMessagingTopologyQuery(family = currentRouteFamilySegment()) {
  return createQuery<MessagingTopologyOverview>({
    key: messagingTopologyQueryKey(family),
    fetch: fetchMessagingTopology(family),
  });
}
