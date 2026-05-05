import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainRealmTable from "@/components/shared/domain-realm-table";
import DomainState from "@/components/shared/domain-state";
import DomainSidebar from "@/components/shared/domain-sidebar";
import PageShell from "@/components/shared/page-shell";
import { createLeaseOverviewQuery } from "@/features/lease/lease-query";

export default function LeasePage() {
  const overview = createLeaseOverviewQuery();
  const data = overview.data;
  const sidebar = data ? (
    <DomainSidebar
      title="Lease snapshot"
      description="Live lease health and realm coverage."
      stats={[
        { label: "Active leases", value: data.stats.leasesActive },
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
          domain="Lease"
          title="Lease overview"
          description="Lease realm coverage and live lease load."
          onRefresh={() => overview.refresh()}
        />

        {overview.loading ? (
          <DomainState kind="loading" message="Loading lease overview..." />
        ) : null}

        {overview.error ? (
          <DomainState
            kind="error"
            message="Lease overview could not be loaded."
            error={overview.error}
          />
        ) : null}

        {data && !overview.loading && !overview.error ? (
          <>
            <DomainMetricTable
              title="Lease metrics"
              metrics={[
                { label: "Active leases", value: data.stats.leasesActive },
                {
                  label: "Ops / sec",
                  value: data.stats.operationsPerSecond.toFixed(2),
                },
              ]}
            />

            <DomainRealmTable
              title="Lease realms"
              realms={data.realms}
              emptyMessage="No lease realms are currently visible."
            />
          </>
        ) : null}
      </section>
    </PageShell>
  );
}
