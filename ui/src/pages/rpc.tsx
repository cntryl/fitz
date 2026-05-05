import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import DomainState from "@/components/shared/domain-state";
import DomainSidebar from "@/components/shared/domain-sidebar";
import PageShell from "@/components/shared/page-shell";
import { createRpcOverviewQuery } from "@/features/rpc/rpc-query";

export default function RpcPage() {
  const overview = createRpcOverviewQuery();
  const data = overview.data;
  const sidebar = data ? (
    <DomainSidebar
      title="RPC snapshot"
      description="Worker registrations and pending request pressure."
      stats={[
        { label: "Workers", value: data.stats.workersRegistered },
        { label: "Requests pending", value: data.stats.requestsPending },
        {
          label: "Ops / sec",
          value: data.stats.operationsPerSecond.toFixed(2),
          note: "Live broker snapshot",
        },
      ]}
    />
  ) : undefined;

  return (
    <PageShell sidebar={sidebar}>
      <section class="domain-page">
        <DomainHeader
          domain="RPC"
          title="RPC overview"
          description="Pending RPC work, worker registrations, and live realm inventory."
          onRefresh={() => overview.refresh()}
        />

        {overview.loading ? <DomainState kind="loading" message="Loading RPC overview..." /> : null}

        {overview.error ? (
          <DomainState
            kind="error"
            message="RPC overview could not be loaded."
            error={overview.error}
          />
        ) : null}

        {data && !overview.loading && !overview.error ? (
          <>
            <DomainMetricTable
              title="RPC metrics"
              metrics={[
                { label: "Workers", value: data.stats.workersRegistered },
                { label: "Requests pending", value: data.stats.requestsPending },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
              ]}
            />

            <DomainRealmTable
              title="RPC realms"
              realms={data.realms}
              emptyMessage="No RPC realms are currently visible."
            />
          </>
        ) : null}
      </section>
    </PageShell>
  );
}
