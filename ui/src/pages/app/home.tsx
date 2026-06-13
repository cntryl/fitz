import { state } from "@askrjs/askr";
import { timer } from "@askrjs/askr/resources";
import { Stack } from "@askrjs/themes/layouts";
import DomainHeader from "@/components/shared/domain-header";
import DomainIndex from "@/components/shared/domain-index";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { TopologyDashboard } from "@/features/topology/topology-dashboard";
import {
  appendTopologyTrendPoint,
  resolveTopologySelection,
} from "@/features/topology/topology-mappers";
import type { TopologyTrendPoint } from "@/features/topology/topology-models";
import { createMessagingTopologyQuery } from "@/features/topology/topology-query";
import { domainLinks } from "@/shared/navigation/domains";

export default function Home() {
  const session = createCurrentSessionQuery();
  const topologyQuery = createMessagingTopologyQuery();
  const [selectedIdState, setSelectedId] = state<string | null>(null);
  const [trendHistoryState, setTrendHistory] = state<TopologyTrendPoint[]>([]);
  const selectedIdValue = selectedIdState();
  const trendHistoryValue = trendHistoryState();

  timer(1_000, () => {
    const current = topologyQuery.data;
    if (!current) {
      return;
    }

    setTrendHistory((currentHistory) => appendTopologyTrendPoint(currentHistory, current));
  });

  if (session.loading && !session.data) {
    return <QueryLoadingState description="Loading admin dashboard..." />;
  }

  if (session.error && !session.data) {
    return <QueryErrorState error={session.error} />;
  }

  const username = session.data?.username ?? "admin";
  const topology = topologyQuery.data;
  const refreshState = topologyQuery.refreshing
    ? "Refreshing"
    : topologyQuery.stale
      ? "Stale"
      : "Last updated";
  const trendHistory = topology
    ? appendTopologyTrendPoint(trendHistoryValue, topology)
    : trendHistoryValue;

  const selectedId = topology
    ? selectedIdValue ?? resolveTopologySelection(topology, null).id
    : "broker";
  const selected = topology ? resolveTopologySelection(topology, selectedId) : null;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          title="Fitz status"
          description={`Welcome, ${username}. Current broker behavior, messaging flow, and attention signals.`}
          onRefresh={() => topologyQuery.refresh()}
        />

        {!topology && topologyQuery.loading ? (
          <QueryLoadingState description="Loading messaging topology..." />
        ) : null}

        {!topology && topologyQuery.error ? <QueryErrorState error={topologyQuery.error} /> : null}

        {topology && selected ? (
          <Stack gap="3">
            <TopologyDashboard
              history={trendHistory}
              isRefreshing={topologyQuery.refreshing}
              refreshState={refreshState}
              selected={selected}
              setSelectedId={setSelectedId}
              topology={topology}
            />

            <DomainIndex
              title="Domain workspaces"
              description="Open a domain when you need a narrower view of resources and live counters."
              links={domainLinks}
            />
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
