import { createQuery, queryScope } from "@askrjs/askr/data";
import { topologyService } from "./topology-service";
import type { MessagingTopologyOverview } from "./topology-models";

const topologyQueries = queryScope("topology");

export const MESSAGING_TOPOLOGY_KEY = topologyQueries.key("overview");

function fetchMessagingTopology({ signal }: { signal: AbortSignal }) {
  return topologyService.getOverview({ signal });
}

export function createMessagingTopologyQuery() {
  return createQuery<MessagingTopologyOverview>({
    key: MESSAGING_TOPOLOGY_KEY,
    fetch: fetchMessagingTopology,
  });
}
