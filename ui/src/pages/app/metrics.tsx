import { state } from "@askrjs/askr";
import { For } from "@askrjs/askr/control";
import { Input, Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Section } from "@askrjs/themes/layouts";
import DomainHeader from "@/components/shared/domain-header";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import { createMetricsOverviewQuery } from "@/features/metrics/metrics-query";

export default function MetricsPage() {
  const metrics = createMetricsOverviewQuery();
  const filter = state("");
  const families =
    metrics.data?.families.filter((family) =>
      family.name.toLowerCase().includes(filter().trim().toLowerCase()),
    ) ?? [];

  return (
    <section class="domain-page">
      <DomainHeader
        domain="Metrics"
        title="Metrics explorer"
        description="Search Prometheus metric families exposed by the Fitz broker."
        onRefresh={() => metrics.refresh()}
      />

      <div class="auth-field metrics-filter">
        <Input
          aria-label="Filter metrics"
          placeholder="Filter metrics"
          value={filter()}
          onInput={(event: Event) => filter.set((event.target as HTMLInputElement).value)}
        />
      </div>

      {metrics.loading ? (
        <QueryLoadingState description="Loading Prometheus metrics..." />
      ) : null}

      {metrics.error ? (
        <QueryErrorState error={metrics.error} />
      ) : null}

      {metrics.data && !metrics.loading && !metrics.error ? (
        <div class="domain-stack">
          <Section class="domain-section" size="3">
            <div class="domain-section-header">
              <div>
                <p class="eyebrow">Metric families</p>
                <h2>{families.length} visible</h2>
              </div>
            </div>
            <div class="domain-table-wrap">
              <Table class="domain-table">
                <TableHead>
                  <TableRow>
                    <TableHeaderCell>Name</TableHeaderCell>
                    <TableHeaderCell>Type</TableHeaderCell>
                    <TableHeaderCell>Samples</TableHeaderCell>
                    <TableHeaderCell>Help</TableHeaderCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  <For each={families} by={(family) => family.name}>
                    {(family) => (
                      <TableRow>
                        <TableCell>{family.name}</TableCell>
                        <TableCell>{family.type ?? "unknown"}</TableCell>
                        <TableCell>{family.samples.length}</TableCell>
                        <TableCell>{family.help ?? "n/a"}</TableCell>
                      </TableRow>
                    )}
                  </For>
                </TableBody>
              </Table>
            </div>
          </Section>

          <Section class="domain-section" size="3">
            <div class="domain-section-header">
              <div>
                <p class="eyebrow">Raw</p>
                <h2>Prometheus payload</h2>
              </div>
            </div>
            <pre class="resource-raw">{metrics.data.raw}</pre>
          </Section>
        </div>
      ) : null}
    </section>
  );
}
