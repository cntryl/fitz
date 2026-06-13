import { Link } from "@askrjs/askr/router";
import { Card, CardContent, CardHeader, CardTitle, Badge } from "@askrjs/themes/surfaces";
import { formatNumber } from "@/shared/format";
import {
  laneTrendDirection,
  topTopologyResources,
  topologyConnectionKindLabel,
  topologyConnectionSelectionId,
  topologyDomainDescriptions,
  topologyLaneIdFromSelectionId,
  topologyLaneSelectionId,
  topologyResourceSelectionId,
  topologySessionGroupSelectionId,
  topologyTrendDirection,
} from "./topology-mappers";
import type {
  MessagingTopologyOverview,
  TopologySelection,
  TopologyTrendPoint,
} from "./topology-models";
import {
  badgeVariant,
  consumerTotal,
  formatTopologyRate,
  stateLabel,
  trendLabel,
} from "./topology-view";

function FlowButton({
  children,
  className,
  isSelected,
  label,
  onSelect,
}: {
  children?: unknown;
  className: string;
  isSelected: boolean;
  key?: string;
  label: string;
  onSelect: () => void;
}) {
  return (
    <button
      type="button"
      class={`${className} flow-selectable${isSelected ? " flow-selected" : ""}`}
      aria-label={label}
      aria-pressed={isSelected}
      onClick={onSelect}
    >
      {children}
    </button>
  );
}

export function MessagingFlow({
  history,
  selected,
  selectedId,
  setSelectedId,
  topology,
}: {
  history: TopologyTrendPoint[];
  selected: TopologySelection;
  selectedId: string;
  setSelectedId: (id: string) => void;
  topology: MessagingTopologyOverview;
}) {
  const resources = topTopologyResources(topology);
  const visibleConnections = topology.connections.items.slice(0, 6);

  return (
    <section class="domain-section topology-section">
      <div class="domain-section-header">
        <div>
          <h2>Messaging flow</h2>
          <p>Connected sessions, Fitz broker lanes, live consumers, and current bottlenecks.</p>
        </div>
        <Badge variant={badgeVariant(selected.state)}>{stateLabel(selected.state)}</Badge>
      </div>

      <div class="flow-stage">
        <div class="flow-map" aria-label="Messaging flow">
          <div class="flow-column flow-column-edge">
            <div class="flow-column-label">Connected sessions</div>
            {topology.sessionGroups.length === 0 ? (
              <div class="flow-node">
                <span>No active sessions</span>
                <strong>0</strong>
                <small>Waiting for broker-visible clients</small>
              </div>
            ) : (
              <div class="flow-session-list">
                {topology.sessionGroups.map((group) => {
                  const id = topologySessionGroupSelectionId(group.routeFamily);

                  return (
                    <FlowButton
                      key={id}
                      className="flow-node flow-session-node"
                      isSelected={selectedId === id}
                      label={`Inspect route family ${group.routeFamily}`}
                      onSelect={() => setSelectedId(id)}
                    >
                      <span>Route family {group.routeFamily}</span>
                      <strong>{formatNumber(group.sessions)} sessions</strong>
                      <small>
                        {formatNumber(group.messagesReceived)} received / {formatNumber(group.messagesSent)} sent
                      </small>
                    </FlowButton>
                  );
                })}
              </div>
            )}
          </div>

          <div class="flow-column flow-column-core">
            <FlowButton
              className="flow-node flow-node-broker"
              isSelected={selectedId === "broker"}
              label="Inspect Fitz broker"
              onSelect={() => setSelectedId("broker")}
            >
              <span>Fitz broker</span>
              <strong>{formatTopologyRate(topology.broker.messagesPerSecond)} msg/sec</strong>
              <small>
                {formatNumber(topology.broker.sessions)} sessions / {formatNumber(topology.broker.realms.length)} realms
              </small>
            </FlowButton>

            <div class="flow-lane-stack">
              {topology.lanes.map((lane) => {
                const id = topologyLaneSelectionId(lane.id);
                const trend = laneTrendDirection(history, lane.id);

                return (
                  <FlowButton
                    key={id}
                    className={`flow-lane flow-lane-${lane.state}`}
                    isSelected={selectedId === id}
                    label={`Inspect ${lane.title}`}
                    onSelect={() => setSelectedId(id)}
                  >
                    <span class="flow-lane-pulse" aria-hidden="true" />
                    <span>
                      <strong>{lane.title}</strong>
                      <small>{topologyDomainDescriptions[lane.id]}</small>
                    </span>
                    <span>
                      <strong>{stateLabel(lane.state)}</strong>
                      <small>{trendLabel(trend)} pressure</small>
                    </span>
                  </FlowButton>
                );
              })}
            </div>
          </div>

          <div class="flow-column flow-column-edge">
            <div class="flow-column-label">Consumers and observers</div>
            <FlowButton
              className="flow-node flow-node-consumer"
              isSelected={false}
              label="Inspect consumers and observers"
              onSelect={() => setSelectedId("broker")}
            >
              <span>Consumers and observers</span>
              <strong>{formatNumber(consumerTotal(topology))}</strong>
              <small>Workers, owners, subscribers, appenders, and live activity</small>
            </FlowButton>

            <div class="flow-resource-list" aria-label="Top scoped resources">
              <span class="flow-column-label">Top scoped resources</span>
              {resources.length === 0 ? (
                <span class="domain-muted">No scoped resources reported.</span>
              ) : (
                resources.map((resource) => (
                  <FlowButton
                    key={resource.id}
                    className={`flow-resource-row flow-lane-${resource.state}`}
                    isSelected={selectedId === topologyResourceSelectionId(resource.id)}
                    label={`Inspect ${resource.label}`}
                    onSelect={() => setSelectedId(topologyResourceSelectionId(resource.id))}
                  >
                    <span>{resource.label}</span>
                    <strong>{resource.domain}</strong>
                  </FlowButton>
                ))
              )}
            </div>

            <div class="flow-connection-list" aria-label="Visible connections">
              <span class="flow-column-label">Visible connections</span>
              {visibleConnections.map((connection) => (
                <FlowButton
                  key={connection.id}
                  className={`flow-connection-row flow-lane-${connection.state}`}
                  isSelected={selectedId === topologyConnectionSelectionId(connection.id)}
                  label={`Inspect ${connection.label}`}
                  onSelect={() => setSelectedId(topologyConnectionSelectionId(connection.id))}
                >
                  <span>{connection.label}</span>
                  <strong>{topologyConnectionKindLabel(connection.kind)}</strong>
                </FlowButton>
              ))}
              {topology.connections.truncated ? (
                <span class="domain-muted">
                  Showing {formatNumber(topology.connections.items.length)} of {formatNumber(topology.connections.total)} connections.
                </span>
              ) : null}
            </div>
          </div>
        </div>

        <TopologyInspector history={history} selected={selected} />
      </div>
    </section>
  );
}

function TopologyInspector({
  history,
  selected,
}: {
  history: TopologyTrendPoint[];
  selected: TopologySelection;
}) {
  const selectedLaneId = selected.kind === "lane" ? topologyLaneIdFromSelectionId(selected.id) : null;
  const trend =
    selected.kind === "broker"
      ? topologyTrendDirection(history, "messagesPerSecond")
      : selectedLaneId
        ? laneTrendDirection(history, selectedLaneId)
        : "stable";

  return (
    <Card class="flow-inspector" padding="sm" variant="default">
      <CardHeader>
        <p class="domain-header-kicker">Flow inspector</p>
        <CardTitle>{selected.title}</CardTitle>
        <p class="domain-muted">{selected.description}</p>
        <div class="flow-inspector-badges">
          <Badge variant={badgeVariant(selected.state)}>{stateLabel(selected.state)}</Badge>
          <Badge variant="info">{trendLabel(trend)}</Badge>
        </div>
      </CardHeader>
      <CardContent>
        <div class="flow-inspector-grid">
          {selected.counters.slice(0, 8).map((counter) => (
            <div key={counter.key}>
              <span>{counter.label}</span>
              <strong>{formatNumber(counter.value)}</strong>
            </div>
          ))}
        </div>

        {"scope" in selected && selected.scope ? (
          <dl class="flow-scope-list">
            {selected.scope.realm ? (
              <>
                <dt>Realm</dt>
                <dd>{selected.scope.realm}</dd>
              </>
            ) : null}
            {selected.scope.routeFamily != null ? (
              <>
                <dt>Route family</dt>
                <dd>{selected.scope.routeFamily}</dd>
              </>
            ) : null}
            {selected.scope.area ? (
              <>
                <dt>Area</dt>
                <dd>{selected.scope.area}</dd>
              </>
            ) : null}
            {selected.scope.resource ? (
              <>
                <dt>Resource</dt>
                <dd>{selected.scope.resource}</dd>
              </>
            ) : null}
            {selected.scope.operation ? (
              <>
                <dt>Operation</dt>
                <dd>{selected.scope.operation}</dd>
              </>
            ) : null}
            {selected.scope.sessionId ? (
              <>
                <dt>Session</dt>
                <dd>{selected.scope.sessionId}</dd>
              </>
            ) : null}
          </dl>
        ) : null}

        {"href" in selected && selected.href ? (
          <Link class="flow-inspector-link" href={selected.href}>
            Open matching view
          </Link>
        ) : null}
      </CardContent>
    </Card>
  );
}
