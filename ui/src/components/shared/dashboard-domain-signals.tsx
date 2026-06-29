import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { ArrowUpRightIcon } from "@askrjs/lucide";
import { Badge } from "@askrjs/themes/components";
import { formatNumber } from "@/shared/format";
import { domainLinks } from "@/shared/navigation/domains";
import { topologyDomainDescriptions } from "@/features/topology/topology-mappers";
import type { MessagingTopologyOverview } from "@/features/topology/topology-models";
import { badgeVariant, formatTopologyRate, stateLabel } from "@/features/topology/topology-view";

const domainLinksByHref = new Map(domainLinks.map((link) => [link.href, link]));

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
        <For each={topology.lanes} by={(lane) => lane.id}>
          {(lane) => {
            const link = domainLinksByHref.get(lane.href);
            const Icon = link?.icon;

            return (
              <li class={`dashboard-signal-item dashboard-signal-item-${lane.state}`}>
                <div class="dashboard-signal-row">
                  <div class="dashboard-signal-body">
                    <div class="dashboard-signal-heading">
                      <div class="dashboard-signal-title-row">
                        {Icon ? <Icon size={16} /> : null}
                        <span class="dashboard-signal-title">{link?.title ?? lane.title}</span>
                      </div>
                      <Badge variant={badgeVariant(lane.state)}>{stateLabel(lane.state)}</Badge>
                    </div>

                    <p class="dashboard-signal-description">
                      {topologyDomainDescriptions[lane.id]}
                    </p>

                    <dl class="dashboard-signal-metrics">
                      <div class="dashboard-signal-metric">
                        <dt>Act/sec</dt>
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
                  </div>
                  <Link
                    href={lane.href}
                    class="dashboard-signal-link"
                    aria-label={`Open ${link?.title ?? lane.title} page`}
                  >
                    <span>Open page</span>
                    <ArrowUpRightIcon size={12} />
                  </Link>
                </div>
              </li>
            );
          }}
        </For>
      </ul>
    </section>
  );
}
