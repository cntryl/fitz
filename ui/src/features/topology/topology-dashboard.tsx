import { Stack } from "@askrjs/themes/layouts";
import { QueryRefreshingState } from "@/components/shared/query-state";
import type {
  MessagingTopologyOverview,
  TopologySelection,
  TopologyTrendPoint,
} from "./topology-models";
import { MessagingFlow } from "./topology-flow";
import { BehaviorMatrix, BrokerStatusStrip, DiagnosticsPanel } from "./topology-panels";

export function TopologyDashboard({
  history,
  isRefreshing,
  refreshState,
  selected,
  setSelectedId,
  topology,
}: {
  history: TopologyTrendPoint[];
  isRefreshing: boolean;
  refreshState: string;
  selected: TopologySelection;
  setSelectedId: (id: string) => void;
  topology: MessagingTopologyOverview;
}) {
  return (
    <Stack gap="3">
      {isRefreshing ? <QueryRefreshingState description="Refreshing messaging topology..." /> : null}

      <BrokerStatusStrip history={history} topology={topology} refreshState={refreshState} />
      <MessagingFlow
        history={history}
        selected={selected}
        selectedId={selected.id}
        setSelectedId={setSelectedId}
        topology={topology}
      />
      <BehaviorMatrix topology={topology} />
      <DiagnosticsPanel topology={topology} />
    </Stack>
  );
}
