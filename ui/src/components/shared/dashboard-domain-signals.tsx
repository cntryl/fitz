import { Link } from "@askrjs/askr/router";
import { ArrowUpRightIcon } from "@askrjs/lucide";
import { Badge } from "@askrjs/themes/surfaces";
import { formatNumber } from "@/shared/format";
import { domainLinks } from "@/shared/navigation/domains";
import { topologyDomainDescriptions } from "@/features/topology/topology-mappers";
import type { MessagingTopologyOverview, TopologyLane } from "@/features/topology/topology-models";
import { badgeVariant, formatTopologyRate, stateLabel } from "@/features/topology/topology-view";

const domainLinksByHref = new Map(domainLinks.map((link) => [link.href, link]));

function visibleCounters(lane: TopologyLane) {
  return lane.counters.slice(0, 2);
}

export default function DashboardDomainSignals({
  topology,
}: {
  topology: MessagingTopologyOverview;
}) {
  return (
    <section class="domain-section dashboard-signal-section" aria-label="Domain signals">
      <div class="domain-section-header">
        <div>
          <h2>Domain signals</h2>
          <p>Comparable drill-downs for each live domain.</p>
        </div>
        <span>{formatNumber(topology.lanes.length)} domains</span>
      </div>

      <ul class="dashboard-signal-list">
        {topology.lanes.map((lane) => {
          const link = domainLinksByHref.get(lane.href);
          const Icon = link?.icon;
          const counters = visibleCounters(lane);

          return (
            <li key={lane.id}>
              <Link
                href={lane.href}
                class={`dashboard-signal-link dashboard-signal-link-${lane.state}`}
              >
                <div class="dashboard-signal-row">
                  <div class="dashboard-signal-heading">
                    <div class="dashboard-signal-title-row">
                      {Icon ? <Icon size={16} /> : null}
                      <span class="dashboard-signal-title">{link?.title ?? lane.title}</span>
                    </div>
                    <Badge variant={badgeVariant(lane.state)}>{stateLabel(lane.state)}</Badge>
                  </div>

                  <p class="dashboard-signal-description">{topologyDomainDescriptions[lane.id]}</p>

                  <dl class="dashboard-signal-metrics">
                    <div class="dashboard-signal-metric">
                      <dt>Activity/sec</dt>
                      <dd>{formatTopologyRate(lane.activityPerSecond)}</dd>
                    </div>
                    <div class="dashboard-signal-metric">
                      <dt>Consumers</dt>
                      <dd>{formatNumber(lane.consumers)}</dd>
                    </div>
                    <div class="dashboard-signal-metric">
                      <dt>Observers</dt>
                      <dd>{formatNumber(lane.observers)}</dd>
                    </div>
                  </dl>

                  <div class="dashboard-signal-counter-grid">
                    {counters.length === 0 ? (
                      <p class="dashboard-signal-empty-counters">No lane counters reported.</p>
                    ) : (
                      counters.map((counter) => (
                        <div class="dashboard-signal-counter" key={`${lane.id}:${counter.key}`}>
                          <span>{counter.label}</span>
                          <strong>{formatNumber(counter.value)}</strong>
                        </div>
                      ))
                    )}
                  </div>

                  <div class="dashboard-signal-footer">
                    <span>Open {link?.title ?? lane.title} page</span>
                    <ArrowUpRightIcon size={14} />
                  </div>
                </div>
              </Link>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
