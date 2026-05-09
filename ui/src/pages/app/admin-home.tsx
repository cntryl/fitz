import { Link } from "@askrjs/askr/router";
import { Button } from "@askrjs/ui";
import {
  Badge,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  SidebarLayout,
} from "@askrjs/themes/components";
import DomainIndex from "@/components/shared/domain-index";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import { AlertTriangleIcon } from "@askrjs/lucide";
import { EmptyState, Spinner } from "@askrjs/themes/components";
import { formatUnknownError } from "@/shared/errors/format";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createSignOutMutation } from "@/features/session/session-mutation";
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

export default function AdminHome() {
  const session = createCurrentSessionQuery();
  const signOut = createSignOutMutation();
  const system = createSystemOverviewQuery();

  async function onSignOut() {
    await signOut.execute(undefined);
    if (typeof window !== "undefined") {
      window.location.replace("/login");
    }
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
        <Button class="secondary-action" onPress={onSignOut}>
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
              This dashboard gives you a broker-level view of system health, throughput, and
              live domain activity so you can see how data is moving through Fitz.
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
          <EmptyState
            class="domain-state"
            icon={<Spinner label="Loading" />}
            description="Loading broker overview..."
          />
        ) : null}

        {system.error ? (
          <EmptyState
            class="domain-state"
            icon={<AlertTriangleIcon size={18} />}
            description={formatUnknownError(system.error)}
          />
        ) : null}

        {overview && !system.loading && !system.error ? (
          <>
            <section class="dashboard-status-grid">
              <Card class="dashboard-status-card" variant="raised">
                <CardHeader>
                  <CardTitle>Broker health</CardTitle>
                  <CardDescription>{overview.healthStatus}</CardDescription>
                </CardHeader>
                <CardContent class="dashboard-status-content">
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
                    <CardDescription>Ready {formatNumber(overview.domains.queue.messagesReady)}</CardDescription>
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
                    <CardDescription>Keys {formatNumber(overview.domains.kv.keysTotal)}</CardDescription>
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
                    <p>Pending claims {formatNumber(overview.domains.schedule.pendingFireClaims)}</p>
                    <p>Executions / min {overview.domains.schedule.executionsPerMinute.toFixed(2)}</p>
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
          </>
        ) : null}
      </section>
    </SidebarLayout>
  );
}