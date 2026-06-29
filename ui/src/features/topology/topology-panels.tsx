import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { VirtualTable, type VirtualTableColumn } from "@askrjs/ui";
import { Alert, Badge, Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/components";
import { formatNumber, formatTimestamp } from "@/shared/format";
import { topologyTrendDirection } from "./topology-mappers";
import type { MessagingTopologyOverview, TopologyTrendPoint } from "./topology-models";
import {
  badgeVariant,
  formatTopologyRate,
  hotspotHref,
  humanizeSeconds,
  incidentDescription,
  incidentSeverity,
  incidentTitle,
  scopeText,
  topologyBehaviorGroups,
  trendLabel,
} from "./topology-view";

export function BrokerStatusStrip({
  history,
  refreshState,
  topology,
}: {
  history: TopologyTrendPoint[];
  refreshState: string;
  topology: MessagingTopologyOverview;
}) {
  const severity = incidentSeverity(topology);
  const messageTrend = topologyTrendDirection(history, "messagesPerSecond");
  const nextQuery =
    topology.diagnostics.incident_summary?.recommended_next_query ?? "No follow-up needed";

  return (
    <Card
      class="dashboard-status-strip"
      variant="raised"
      role="region"
      aria-label="Broker snapshot"
    >
      <div class="dashboard-status-summary">
        <div>
          <p class="domain-header-kicker">Current snapshot</p>
          <h2>{incidentTitle(topology)}</h2>
          <p>{incidentDescription(topology)}</p>
          <p class="domain-muted">Next: {nextQuery}</p>
        </div>
        <Badge variant={badgeVariant(severity)}>{severity}</Badge>
      </div>

      <dl class="dashboard-status-metrics">
        <div>
          <dt>Uptime</dt>
          <dd>{humanizeSeconds(topology.broker.uptimeSeconds)}</dd>
        </div>
        <div>
          <dt>Sessions</dt>
          <dd>{formatNumber(topology.broker.sessions)}</dd>
        </div>
        <div>
          <dt>Connections</dt>
          <dd>{formatNumber(topology.broker.connections)}</dd>
        </div>
        <div>
          <dt>Messages/sec</dt>
          <dd>{formatTopologyRate(topology.broker.messagesPerSecond)}</dd>
        </div>
        <div>
          <dt>Realms</dt>
          <dd>{formatNumber(topology.broker.realms.length)}</dd>
        </div>
        <div>
          <dt>Router pressure</dt>
          <dd>{formatNumber(topology.broker.routerBackpressureTotal)}</dd>
        </div>
        <div>
          <dt>{refreshState}</dt>
          <dd>{formatTimestamp(topology.generatedAt)}</dd>
        </div>
        <div>
          <dt>Trend</dt>
          <dd>{trendLabel(messageTrend)}</dd>
        </div>
      </dl>
    </Card>
  );
}

export function BehaviorMatrix({ topology }: { topology: MessagingTopologyOverview }) {
  return (
    <section class="domain-section">
      <div class="domain-section-header">
        <div>
          <h2>Behavior groups</h2>
          <p>Scan current pressure by what the broker is doing.</p>
        </div>
      </div>

      <div class="dashboard-behavior-grid">
        <For each={topologyBehaviorGroups(topology)} by={(group) => group.title}>
          {(group) => (
            <Card padding="sm" variant="default">
              <CardHeader>
                <CardTitle>{group.title}</CardTitle>
                <p class="domain-muted">{group.description}</p>
              </CardHeader>
              <CardContent>
                <div class="dashboard-behavior-list">
                  <For each={group.rows} by={(row) => row.lane.id}>
                    {(row) => (
                      <Link href={row.lane.href} class="dashboard-behavior-row">
                        <span>{row.lane.title}</span>
                        <strong>{row.primary}</strong>
                        <small>{row.secondary}</small>
                      </Link>
                    )}
                  </For>
                </div>
              </CardContent>
            </Card>
          )}
        </For>
      </div>
    </section>
  );
}

export function DiagnosticsPanel({ topology }: { topology: MessagingTopologyOverview }) {
  const severity = incidentSeverity(topology);
  const hotspots = topology.diagnostics.hotspots.slice(0, 6);
  const isMeaningful = severity !== "informational" || hotspots.length > 0;
  const hotspotColumns: readonly VirtualTableColumn<(typeof hotspots)[number]>[] = [
    {
      id: "scope",
      header: "Scope",
      width: "32%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={scopeText(row) || "Broker"}>
          {scopeText(row) || "Broker"}
        </span>
      ),
    },
    {
      id: "severity",
      header: "Severity",
      width: "18%",
      cellComponent: ({ row }) => <span>{row.severity}</span>,
    },
    {
      id: "stage",
      header: "Stage",
      width: "26%",
      cellComponent: ({ row }) => (
        <span class="domain-table-cell-truncate" title={row.current_stage}>
          {row.current_stage}
        </span>
      ),
    },
    {
      id: "action",
      header: "Next step",
      width: "24%",
      cellComponent: ({ row }) => {
        const href = hotspotHref(row);

        return href ? <Link href={href}>Open scope</Link> : <span>Review diagnostics</span>;
      },
    },
  ];

  if (!isMeaningful) {
    return null;
  }

  const alertVariant =
    severity === "critical" || severity === "high"
      ? "danger"
      : severity === "medium" || severity === "low"
        ? "warning"
        : "success";

  return (
    <section class="domain-section">
      <Alert variant={alertVariant} title="Attention" description={incidentDescription(topology)} />

      {hotspots.length > 0 ? (
        <VirtualTable<(typeof hotspots)[number]>
          aria-label="Dashboard diagnostic hotspots"
          class="dashboard-hotspot-virtual-table"
          columns={hotspotColumns}
          getKey={(hotspot) =>
            `${hotspot.domain ?? "unknown"}:${hotspot.realm ?? "any"}:${hotspot.area ?? "any"}:${hotspot.resource ?? "any"}`
          }
          headerHeight={44}
          overscan={4}
          rowHeight={48}
          rows={hotspots}
          style={{ height: `${Math.min(360, Math.max(144, 44 + hotspots.length * 48))}px` }}
        />
      ) : null}
    </section>
  );
}
