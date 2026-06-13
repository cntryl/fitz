import { createQuery } from "@askrjs/askr/data";
import { topologyService } from "./topology-service";
import type { MessagingTopologyOverview } from "./topology-models";

export const MESSAGING_TOPOLOGY_KEY = "system:topology";

function fetchMessagingTopology({ signal }: { signal: AbortSignal }) {
  return topologyService.getOverview({ signal });
}

export function createMessagingTopologyQuery() {
  return createQuery<MessagingTopologyOverview>({
    key: MESSAGING_TOPOLOGY_KEY,
    fetch: fetchMessagingTopology,
  });
}
