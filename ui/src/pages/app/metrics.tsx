import { state } from "@askrjs/askr";
import {
  Input,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeaderCell,
  TableRow,
} from "@askrjs/ui";
import { Stack } from "@askrjs/themes/layouts";
import { Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createMetricsOverviewQuery } from "@/features/metrics/metrics-query";

export default function MetricsPage() {
  const metrics = createMetricsOverviewQuery();
  const [filter, setFilter] = state("");
  const data = metrics.data;
  const filterValue = filter();
  const families =
    data?.families.filter((family) =>
      family.name.toLowerCase().includes(filterValue.trim().toLowerCase()),
    ) ?? [];

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          title="Metrics explorer"
          description="Search Prometheus metric families exposed by the Fitz broker."
          onRefresh={() => metrics.refresh()}
        />

        <section class="domain-section">
          <div class="domain-section-header">
            <h2>Filter metrics</h2>
          </div>
          <div class="auth-field metrics-filter">
            <Input
              aria-label="Filter metrics"
              placeholder="Filter metrics"
              value={filterValue}
              onInput={(event: Event) => setFilter((event.target as HTMLInputElement).value)}
            />
          </div>
        </section>

        {!data && metrics.loading ? (
          <QueryLoadingState description="Loading Prometheus metrics..." />
        ) : null}

        {!data && metrics.error ? <QueryErrorState error={metrics.error} /> : null}

        {data ? (
          <Stack gap="3">
            {metrics.refreshing ? (
              <QueryRefreshingState description="Refreshing Prometheus metrics..." />
            ) : null}

            <Card class="metrics-family-card" padding="sm" variant="default">
              <CardHeader>
                <CardTitle>Metric families</CardTitle>
                <p class="domain-muted">{families.length} visible</p>
              </CardHeader>
              <CardContent>
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
                      {families.map((family) => (
                        <TableRow key={family.name}>
                          <TableCell>{family.name}</TableCell>
                          <TableCell>{family.type ?? "unknown"}</TableCell>
                          <TableCell>{family.samples.length}</TableCell>
                          <TableCell>{family.help ?? "n/a"}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>

            <section class="domain-section">
              <div class="domain-section-header">
                <h2>Prometheus payload</h2>
              </div>
              <pre class="resource-raw">{data.raw}</pre>
            </section>
          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
