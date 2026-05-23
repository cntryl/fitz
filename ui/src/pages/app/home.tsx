import { For } from "@askrjs/askr/control";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Stack } from "@askrjs/themes/layouts";
import DashboardDomainSignals from "@/components/shared/dashboard-domain-signals";
import DomainHeader from "@/components/shared/domain-header";
import DomainMetricTable from "@/components/shared/domain-metric-table";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import {
  QueryErrorState,
  QueryLoadingState,
  QueryRefreshingState,
} from "@/components/shared/query-state";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createSystemOverviewQuery } from "@/features/system/system-query";
import { formatNumber } from "@/shared/format";

function humanizeSeconds(seconds: number) {
  if (seconds < 60) return `${seconds}s`;

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;

  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h`;

  return `${Math.floor(hours / 24)}d`;
}

export default function Home() {
  const session = createCurrentSessionQuery();
  const system = createSystemOverviewQuery();

  if (session.loading && !session.data) {
    return <QueryLoadingState description="Loading admin dashboard..." />;
  }

  if (session.error && !session.data) {
    return <QueryErrorState error={session.error} />;
  }

  const username = session.data?.username ?? "admin";
  const overview = system.data;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          title={`Welcome, ${username}`}
          description="Broker status, domain totals, and admin entry points."
          onRefresh={() => system.refresh()}
        />

        {!overview && system.loading ? (
          <QueryLoadingState description="Loading broker overview..." />
        ) : null}

        {!overview && system.error ? <QueryErrorState error={system.error} /> : null}

        {overview ? (
          <Stack gap="3">
            {system.refreshing ? (
              <QueryRefreshingState description="Refreshing broker overview..." />
            ) : null}

            <DomainMetricTable
              title="Broker"
              metrics={[
                { label: "Uptime", value: humanizeSeconds(overview.broker.uptimeSeconds) },
                { label: "Connections", value: formatNumber(overview.broker.connections) },
                { label: "Sessions", value: formatNumber(overview.broker.sessions) },
                { label: "Messages / sec", value: overview.broker.messagesPerSecond.toFixed(2) },
                {
                  label: "Incident",
                  value: overview.diagnostics.incident_summary?.title ?? "No incident detected",
                },
              ]}
            />

            <DashboardDomainSignals overview={overview} />

            <section class="domain-section">
              <div class="domain-section-header">
                <h2>Domains</h2>
              </div>
              <div class="domain-table-wrap">
                <Table class="domain-table">
                  <TableHead>
                    <TableRow>
                      <TableHeaderCell>Domain</TableHeaderCell>
                      <TableHeaderCell>Primary</TableHeaderCell>
                      <TableHeaderCell>Secondary</TableHeaderCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    <TableRow>
                      <TableCell>Queue</TableCell>
                      <TableCell>
                        Ready {formatNumber(overview.domains.queue.messagesReady)}
                      </TableCell>
                      <TableCell>
                        Dead letters {formatNumber(overview.domains.queue.messagesDeadLettered)}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>KV</TableCell>
                      <TableCell>Keys {formatNumber(overview.domains.kv.keysTotal)}</TableCell>
                      <TableCell>
                        Transactions {formatNumber(overview.domains.kv.transactionsActive)}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>Lease</TableCell>
                      <TableCell>
                        Active {formatNumber(overview.domains.lease.leasesActive)}
                      </TableCell>
                      <TableCell>
                        Ops / sec {overview.domains.lease.operationsPerSecond.toFixed(2)}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>Notice</TableCell>
                      <TableCell>
                        Subscriptions {formatNumber(overview.domains.notice.subscriptionsActive)}
                      </TableCell>
                      <TableCell>
                        Publishes / sec {overview.domains.notice.publishesPerSecond.toFixed(2)}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>RPC</TableCell>
                      <TableCell>
                        Workers {formatNumber(overview.domains.rpc.workersRegistered)}
                      </TableCell>
                      <TableCell>
                        Pending {formatNumber(overview.domains.rpc.requestsPending)}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>Schedule</TableCell>
                      <TableCell>
                        Active {formatNumber(overview.domains.schedule.schedulesActive)}
                      </TableCell>
                      <TableCell>
                        Claims {formatNumber(overview.domains.schedule.pendingFireClaims)}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>Stream</TableCell>
                      <TableCell>
                        Streams {formatNumber(overview.domains.stream.streamsActive)}
                      </TableCell>
                      <TableCell>
                        Events {formatNumber(overview.domains.stream.eventsTotal)}
                      </TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </div>
            </section>

            <section class="domain-section">
              <div class="domain-section-header">
                <h2>Recent signals</h2>
                <span>{overview.diagnostics.hotspots.length} hotspots</span>
              </div>
              {overview.diagnostics.hotspots.length === 0 ? (
                <p class="domain-muted">No active hotspots reported.</p>
              ) : (
                <div class="domain-table-wrap">
                  <Table class="domain-table">
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Scope</TableHeaderCell>
                        <TableHeaderCell>Severity</TableHeaderCell>
                        <TableHeaderCell>Stage</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      <For
                        each={overview.diagnostics.hotspots.slice(0, 6)}
                        by={(hotspot) =>
                          `${hotspot.domain ?? "unknown"}:${hotspot.realm ?? "any"}:${hotspot.area ?? "any"}:${hotspot.resource ?? "any"}`
                        }
                      >
                        {(hotspot) => (
                          <TableRow>
                            <TableCell>
                              {[hotspot.domain, hotspot.realm, hotspot.area, hotspot.resource]
                                .filter(Boolean)
                                .join(" / ") || "Broker"}
                            </TableCell>
                            <TableCell>{hotspot.severity}</TableCell>
                            <TableCell>{hotspot.current_stage}</TableCell>
                          </TableRow>
                        )}
                      </For>
                    </TableBody>
                  </Table>
                </div>
              )}
            </section>

          </Stack>
        ) : null}
      </Stack>
    </DomainPageFrame>
  );
}
