import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { Table, TableBody, TableCell, TableHead, TableHeaderCell, TableRow } from "@askrjs/ui";
import { Badge, Card, CardContent, CardHeader, CardTitle } from "@askrjs/themes/surfaces";
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
  const nextQuery = topology.diagnostics.incident_summary?.recommended_next_query ?? "No follow-up needed";

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
        {topologyBehaviorGroups(topology).map((group) => (
          <Card key={group.title} padding="sm" variant="default">
            <CardHeader>
              <CardTitle>{group.title}</CardTitle>
              <p class="domain-muted">{group.description}</p>
            </CardHeader>
            <CardContent>
              <div class="dashboard-behavior-list">
                {group.rows.map((row) => (
                  <Link key={row.lane.id} href={row.lane.href} class="dashboard-behavior-row">
                    <span>{row.lane.title}</span>
                    <strong>{row.primary}</strong>
                    <small>{row.secondary}</small>
                  </Link>
                ))}
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    </section>
  );
}

export function DiagnosticsPanel({ topology }: { topology: MessagingTopologyOverview }) {
  const hotspots = topology.diagnostics.hotspots.slice(0, 6);

  return (
    <section class="domain-section">
      <div class="domain-section-header">
        <div>
          <h2>Attention</h2>
          <p>{incidentDescription(topology)}</p>
        </div>
        <span>{hotspots.length} hotspots</span>
      </div>

      {hotspots.length === 0 ? (
        <p class="domain-muted">No active hotspots reported.</p>
      ) : (
        <div class="domain-table-wrap">
          <Table>
            <TableHead>
              <TableRow>
                <TableHeaderCell>Scope</TableHeaderCell>
                <TableHeaderCell>Severity</TableHeaderCell>
                <TableHeaderCell>Stage</TableHeaderCell>
                <TableHeaderCell>Next step</TableHeaderCell>
              </TableRow>
            </TableHead>
            <TableBody>
              <For
                each={hotspots}
                by={(hotspot) =>
                  `${hotspot.domain ?? "unknown"}:${hotspot.realm ?? "any"}:${hotspot.area ?? "any"}:${hotspot.resource ?? "any"}`
                }
              >
                {(hotspot) => {
                  const href = hotspotHref(hotspot);

                  return (
                    <TableRow>
                      <TableCell>{scopeText(hotspot) || "Broker"}</TableCell>
                      <TableCell>{hotspot.severity}</TableCell>
                      <TableCell>{hotspot.current_stage}</TableCell>
                      <TableCell>
                        {href ? <Link href={href}>Open scope</Link> : "Review diagnostics"}
                      </TableCell>
                    </TableRow>
                  );
                }}
              </For>
            </TableBody>
          </Table>
        </div>
      )}
    </section>
  );
}
