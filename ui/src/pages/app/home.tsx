import { Link, navigate } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/controls";
import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import DomainIndex from "@/components/shared/domain-index";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import SidebarLayout from "@/components/shared/sidebar-layout";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createHealthSummaryQuery } from "@/features/system/health-query";
import { createSystemOverviewQuery } from "@/features/system/system-query";
import { domainLinks } from "@/shared/navigation/domains";

function humanizeSeconds(seconds: number) {
  if (seconds < 60) {
    return `${seconds}s`;
  }

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m`;
  }

  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours}h`;
  }

  return `${Math.floor(hours / 24)}d`;
}

function formatNumber(value: number) {
  return new Intl.NumberFormat("en-US").format(value);
}

function formatBottleneckLabel(
  bottleneck: { domain?: string | null; resource?: string | null; area?: string | null; realm?: string | null } | null | undefined,
) {
  if (!bottleneck) {
    return "No active bottleneck";
  }

  const parts = [bottleneck.domain, bottleneck.realm, bottleneck.area, bottleneck.resource].filter(
    (part): part is string => Boolean(part),
  );

  return parts.length > 0 ? parts.join(" / ") : "Unnamed bottleneck";
}

export default function Home() {
  const session = createCurrentSessionQuery();
  const health = createHealthSummaryQuery();
  const system = createSystemOverviewQuery();

  function onLogout() {
    navigate("/logout");
  }

  if (session.loading) {
    return (
      <section class="admin-panel">
        <p>Loading admin dashboard...</p>
      </section>
    );
  }

  if (session.error) {
    return (
      <section class="admin-panel">
        <h1>Admin session unavailable</h1>
        <p>We could not load your admin session right now.</p>
      </section>
    );
  }

  const username = session.data?.username ?? "admin";

  const overview = system.data;
  const incident = overview?.diagnostics.incident_summary;
  const topBottleneck = overview?.diagnostics.top_bottleneck;

  const sidebar = createDomainSidebar({
    data: overview,
    title: "Broker snapshot",
    description: "Current state of the live Fitz broker and admin access.",
    stats: (current) => [
      {
        label: "Health",
        value: current.healthStatus,
        note: "Latest health endpoint result",
      },
      { label: "Connections", value: current.broker.connections },
      { label: "Sessions", value: current.broker.sessions },
      { label: "Realms", value: current.broker.realms.length },
      { label: "Uptime", value: humanizeSeconds(current.broker.uptimeSeconds) },
    ],
    footer: (
      <div class="admin-sidebar-actions">
        <Link href="/sessions" class="admin-sidebar-link">
          View sessions
        </Link>
        <Button class="secondary-action" onPress={onLogout}>
          Sign out
        </Button>
      </div>
    ),
  });

  return (
    <SidebarLayout
      sidebar={sidebar}
      sidebarPosition="end"
      sidebarWidth="18rem"
      gap="1.5rem"
      collapseBelow="md"
    >
      <section class="domain-page">
        <div class="admin-hero">
          <div class="panel-heading">
            <Badge>Authenticated</Badge>
            <p class="eyebrow">Admin Home</p>
          </div>

          <div class="panel-copy">
            <h1>Welcome, {username}</h1>
            <p>
              This troubleshooting view leads with the current incident summary, then drills into
              the broker and domains so you can see what is blocked first.
            </p>
          </div>

          <div class="admin-actions">
            <Button class="secondary-action" onPress={() => system.refresh()}>
              Refresh broker data
            </Button>
            <Link href="/sessions" class="admin-sidebar-link">
              Open sessions
            </Link>
          </div>
        </div>

        {system.loading ? (
          <QueryLoadingState description="Loading broker overview..." />
        ) : null}

        {system.error ? (
          <QueryErrorState error={system.error} />
        ) : null}

        {overview && !system.loading && !system.error ? (
          <div class="domain-stack">
            <section class="dashboard-status-grid">
              <Card class="dashboard-status-card" variant="raised">
                <CardHeader>
                  <CardTitle>Current incident</CardTitle>
                  <CardDescription>{incident?.status ?? "unknown"}</CardDescription>
                </CardHeader>
                <CardContent class="dashboard-status-content">
                  <div class="dashboard-metric">
                    <span>Summary</span>
                    <strong>{incident?.title ?? "No incident detected"}</strong>
                  </div>
                  <div class="dashboard-metric">
                    <span>Likely cause</span>
                    <strong>{incident?.explanation ?? "No active pressure detected"}</strong>
                  </div>
                  <div class="dashboard-metric">
                    <span>Top bottleneck</span>
                    <strong>{formatBottleneckLabel(topBottleneck)}</strong>
                  </div>
                  <div class="dashboard-metric">
                    <span>Next query</span>
                    <strong>{incident?.recommended_next_query ?? "No follow-up needed"}</strong>
                  </div>
                </CardContent>
              </Card>

              <Card class="dashboard-status-card" variant="raised">
                <CardHeader>
                  <CardTitle>Broker health</CardTitle>
                  <CardDescription>{health.data?.readiness ?? overview.healthStatus}</CardDescription>
                </CardHeader>
                <CardContent class="dashboard-status-content">
                  <div class="dashboard-metric">
                    <span>Liveness</span>
                    <strong>{health.data?.liveness ?? "checking"}</strong>
                  </div>
                  <div class="dashboard-metric">
                    <span>Startup</span>
                    <strong>{health.data?.startup ?? "checking"}</strong>
                  </div>
                  <div class="dashboard-metric">
                    <span>Uptime</span>
                    <strong>{humanizeSeconds(overview.broker.uptimeSeconds)}</strong>
                  </div>
                  <div class="dashboard-metric">
                    <span>Connections</span>
                    <strong>{formatNumber(overview.broker.connections)}</strong>
                  </div>
                  <div class="dashboard-metric">
                    <span>Sessions</span>
                    <strong>{formatNumber(overview.broker.sessions)}</strong>
                  </div>
                  <div class="dashboard-metric">
                    <span>Messages / sec</span>
                    <strong>{overview.broker.messagesPerSecond.toFixed(2)}</strong>
                  </div>
                </CardContent>
              </Card>

              <Card class="dashboard-status-card" variant="raised">
                <CardHeader>
                  <CardTitle>Metrics preview</CardTitle>
                  <CardDescription>{overview.metrics.lineCount} visible lines</CardDescription>
                </CardHeader>
                <CardContent class="dashboard-metrics-preview">
                  {overview.metrics.lines.length === 0 ? (
                    <p>No metrics payload was returned.</p>
                  ) : (
                    <pre>{overview.metrics.lines.join("\n")}</pre>
                  )}
                </CardContent>
              </Card>
            </section>

            <section class="domain-section">
              <div class="domain-section-header">
                <div>
                  <p class="eyebrow">Recent signals</p>
                  <h2>{overview.diagnostics.hotspots.length} hotspots</h2>
                  <p>Current troubleshooting hotspots ranked by the broker diagnostics model.</p>
                </div>
              </div>

              <div class="dashboard-domain-grid">
                {overview.diagnostics.hotspots.slice(0, 6).map((hotspot) => (
                  <Card class="dashboard-domain-card" variant="raised">
                    <CardHeader>
                      <CardTitle>{formatBottleneckLabel(hotspot)}</CardTitle>
                      <CardDescription>{hotspot.severity}</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <p>{hotspot.current_stage}</p>
                      <p>{hotspot.likely_bottleneck ?? "No likely bottleneck reported"}</p>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </section>

            <section class="dashboard-domains">
              <div class="domain-section-header">
                <div>
                  <p class="eyebrow">Domain totals</p>
                  <h2>High-signal broker summaries</h2>
                </div>
              </div>

              <div class="dashboard-domain-grid">
                <Card class="dashboard-domain-card">
                  <CardHeader>
                    <CardTitle>Queue</CardTitle>
                    <CardDescription>
                      Ready {formatNumber(overview.domains.queue.messagesReady)}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <p>Inflight {formatNumber(overview.domains.queue.inflightActive)}</p>
                    <p>Pending {formatNumber(overview.domains.queue.messagesPending)}</p>
                    <p>Dead letters {formatNumber(overview.domains.queue.messagesDeadLettered)}</p>
                  </CardContent>
                </Card>

                <Card class="dashboard-domain-card">
                  <CardHeader>
                    <CardTitle>KV</CardTitle>
                    <CardDescription>
                      Keys {formatNumber(overview.domains.kv.keysTotal)}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <p>Transactions {formatNumber(overview.domains.kv.transactionsActive)}</p>
                    <p>Ops / sec {overview.domains.kv.operationsPerSecond.toFixed(2)}</p>
                  </CardContent>
                </Card>

                <Card class="dashboard-domain-card">
                  <CardHeader>
                    <CardTitle>Lease</CardTitle>
                    <CardDescription>
                      Active {formatNumber(overview.domains.lease.leasesActive)}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <p>Ops / sec {overview.domains.lease.operationsPerSecond.toFixed(2)}</p>
                  </CardContent>
                </Card>

                <Card class="dashboard-domain-card">
                  <CardHeader>
                    <CardTitle>Notice</CardTitle>
                    <CardDescription>
                      Subscriptions {formatNumber(overview.domains.notice.subscriptionsActive)}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <p>Publishes / sec {overview.domains.notice.publishesPerSecond.toFixed(2)}</p>
                  </CardContent>
                </Card>

                <Card class="dashboard-domain-card">
                  <CardHeader>
                    <CardTitle>RPC</CardTitle>
                    <CardDescription>
                      Workers {formatNumber(overview.domains.rpc.workersRegistered)}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <p>Requests pending {formatNumber(overview.domains.rpc.requestsPending)}</p>
                    <p>Ops / sec {overview.domains.rpc.operationsPerSecond.toFixed(2)}</p>
                  </CardContent>
                </Card>

                <Card class="dashboard-domain-card">
                  <CardHeader>
                    <CardTitle>Schedule</CardTitle>
                    <CardDescription>
                      Active {formatNumber(overview.domains.schedule.schedulesActive)}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <p>
                      Pending claims {formatNumber(overview.domains.schedule.pendingFireClaims)}
                    </p>
                    <p>
                      Executions / min {overview.domains.schedule.executionsPerMinute.toFixed(2)}
                    </p>
                  </CardContent>
                </Card>

                <Card class="dashboard-domain-card">
                  <CardHeader>
                    <CardTitle>Stream</CardTitle>
                    <CardDescription>
                      Active {formatNumber(overview.domains.stream.streamsActive)}
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <p>Events {formatNumber(overview.domains.stream.eventsTotal)}</p>
                    <p>Subscriptions {formatNumber(overview.domains.stream.subscriptionsActive)}</p>
                    <p>Ops / sec {overview.domains.stream.operationsPerSecond.toFixed(2)}</p>
                  </CardContent>
                </Card>
              </div>
            </section>

            <DomainIndex
              title="Domain workbench"
              description="Use the domain pages for deeper inspection and queue resource drill-downs."
              links={domainLinks}
            />
          </div>
        ) : null}
      </section>
    </SidebarLayout>
  );
}
