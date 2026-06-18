import { EmptyState } from "@askrjs/themes/feedback";
import { Rows3Icon } from "@askrjs/lucide";
import DashboardDomainSignals from "@/components/shared/dashboard-domain-signals";
import DomainIndex from "@/components/shared/domain-index";
import { domainLinks } from "@/shared/navigation/domains";
import { Stack } from "@askrjs/themes/layouts";
import type {
  MessagingTopologyOverview,
  TopologySelection,
  TopologyTrendPoint,
} from "./topology-models";
import { MessagingFlow } from "./topology-flow";
import { BrokerStatusStrip, DiagnosticsPanel } from "./topology-panels";

export function TopologyDashboard({
  history,
  refreshState,
  selected,
  setSelectedId,
  topology,
}: {
  history: TopologyTrendPoint[];
  refreshState: string;
  selected: TopologySelection;
  setSelectedId: (id: string) => void;
  topology: MessagingTopologyOverview;
}) {
  return (
    <Stack gap="3">
      <DiagnosticsPanel topology={topology} />
      <BrokerStatusStrip history={history} topology={topology} refreshState={refreshState} />

      {topology.lanes.length === 0 ? (
        <Stack gap="3">
          <div class="domain-state">
            <EmptyState
              icon={<Rows3Icon size={28} />}
              title="No domain lanes are visible yet"
              description="The broker snapshot loaded, but there is no lane breakdown to inspect. Use the domain pages below or refresh the topology."
            />
          </div>

          <DomainIndex
            title="Domain workspaces"
            description="Open a domain when you need a narrower view of resources and live counters."
            links={domainLinks}
          />
        </Stack>
      ) : (
        <>
          <MessagingFlow
            history={history}
            selected={selected}
            selectedId={selected.id}
            setSelectedId={setSelectedId}
            topology={topology}
          />
          <DashboardDomainSignals topology={topology} />
        </>
      )}
    </Stack>
  );
}
