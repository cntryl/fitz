import { For, Show } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { ArrowUpRightIcon, CheckCircle2Icon, CircleAlertIcon } from "@askrjs/lucide";
import { Stack } from "@askrjs/themes/components";
import { Alert, Badge } from "@askrjs/themes/components";
import DomainHeader from "@/components/shared/domain-header";
import DomainPageFrame from "@/components/shared/domain-page-frame";
import OperatorScopeStrip from "@/components/shared/operator-scope-strip";
import { QueryErrorState, QueryLoadingState } from "@/components/shared/query-state";
import {
  buildOverviewStatus,
  type OverviewStatus,
  type OverviewTone,
} from "@/features/overview/overview-status";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createSystemOverviewQuery } from "@/features/system/system-query";
import { createMessagingTopologyQuery } from "@/features/topology/topology-query";
import { formatRelativeTime } from "@/shared/format";
import { formatUnknownError } from "@/shared/errors/format";

function toneVariant(tone: OverviewTone) {
  if (tone === "danger") return "danger";
  if (tone === "warning") return "warning";
  if (tone === "success") return "success";
  return "info";
}

function OverviewStatusBand({ overview }: { overview: OverviewStatus }) {
  const Icon = overview.overall.tone === "success" ? CheckCircle2Icon : CircleAlertIcon;

  return (
    <section class={`overview-status-band overview-status-band-${overview.overall.tone}`}>
      <div class="overview-status-icon">
        <Icon size={22} />
      </div>
      <div class="overview-status-copy">
        <p class="domain-header-kicker">Current status</p>
        <h2>{overview.overall.title}</h2>
        <p>{overview.overall.description}</p>
      </div>
      <Badge variant={toneVariant(overview.overall.tone)}>{overview.overall.label}</Badge>
    </section>
  );
}

function OverviewIssues({ overview }: { overview: OverviewStatus }) {
  return (
    <section class="domain-section overview-issues-section" aria-label="Actionable issues">
      <div class="domain-section-header">
        <div>
          <h2>Issues</h2>
          <p>Actionable signals only. Benign counters and informational diagnostics stay out.</p>
        </div>
        <span>{overview.issues.length} active</span>
      </div>

      <Show when={overview.issues.length === 0}>
        <div class="overview-empty-state">
          {overview.complete ? <CheckCircle2Icon size={18} /> : <CircleAlertIcon size={18} />}
          <div>
            <strong>{overview.complete ? "No active issues" : "Issue status incomplete"}</strong>
            <p>
              {overview.complete
                ? "Open a domain below when you need to inspect normal activity or resource detail."
                : "Load both topology and system counters before treating the selected Route Family as healthy."}
            </p>
          </div>
        </div>
      </Show>
      <Show when={overview.issues.length > 0}>
        <ol class="overview-issue-list">
          <For each={overview.issues} by={(issue) => issue.id}>
            {(issue) => (
              <li class={`overview-issue-item overview-issue-item-${issue.tone}`}>
                <div class="overview-issue-main">
                  <div class="overview-issue-heading">
                    <strong>{issue.title}</strong>
                    <Badge variant={toneVariant(issue.tone)}>{issue.severity}</Badge>
                  </div>
                  <p>{issue.description}</p>
                  <span>{issue.scope}</span>
                </div>
                <Link href={issue.href} class="overview-action-link">
                  <span>{issue.action}</span>
                  <ArrowUpRightIcon size={13} />
                </Link>
              </li>
            )}
          </For>
        </ol>
      </Show>
    </section>
  );
}

function DomainHealth({ overview }: { overview: OverviewStatus }) {
  return (
    <section class="domain-section" aria-label="Domain health">
      <div class="domain-section-header">
        <div>
          <h2>Domain health</h2>
          <p>Compact state by Fitz domain, with the next useful drill-down.</p>
        </div>
      </div>

      <ul class="overview-domain-grid">
        <For each={overview.domains} by={(domain) => domain.href}>
          {(domain) => (
            <li class={`overview-domain-card overview-domain-card-${domain.tone}`}>
              <div class="overview-domain-heading">
                <strong>{domain.title}</strong>
                <Badge variant={toneVariant(domain.tone)}>{domain.state}</Badge>
              </div>
              <p>{domain.signal}</p>
              <Link
                href={domain.href}
                class="overview-action-link"
                aria-label={`Open ${domain.title} inventory`}
              >
                <span>Open {domain.title}</span>
                <ArrowUpRightIcon size={13} />
              </Link>
            </li>
          )}
        </For>
      </ul>
    </section>
  );
}

function BrokerVitals({ overview }: { overview: OverviewStatus }) {
  return (
    <section class="domain-section" aria-label="Broker vitals">
      <div class="domain-section-header">
        <div>
          <h2>Broker vitals</h2>
          <p>The few broker-wide numbers that help frame the issue list.</p>
        </div>
      </div>

      <dl class="overview-vitals-grid">
        <For each={overview.vitals} by={(vital) => vital.label}>
          {(vital) => (
            <div>
              <dt>{vital.label}</dt>
              <dd>{vital.value}</dd>
              {vital.caption ? <small>{vital.caption}</small> : null}
            </div>
          )}
        </For>
      </dl>
    </section>
  );
}

export default function Home() {
  const session = createCurrentSessionQuery();
  const topologyQuery = createMessagingTopologyQuery();
  const systemQuery = createSystemOverviewQuery();

  if (session.loading && !session.data) {
    return (
      <DomainPageFrame>
        <QueryLoadingState description="Loading admin dashboard..." />
      </DomainPageFrame>
    );
  }

  if (session.error && !session.data) {
    return (
      <DomainPageFrame>
        <QueryErrorState error={session.error} onRetry={() => session.refresh()} />
      </DomainPageFrame>
    );
  }

  const topology = topologyQuery.data;
  const system = systemQuery.data;
  const overview = buildOverviewStatus({ system, topology });
  const hasOperationalData = Boolean(topology || system);
  const sourceUnavailable =
    (!topology && Boolean(topologyQuery.error)) || (!system && Boolean(systemQuery.error));
  const refreshState =
    topologyQuery.refreshing || systemQuery.refreshing
      ? "Refreshing"
      : topologyQuery.stale || systemQuery.stale
        ? "Stale"
        : sourceUnavailable
          ? hasOperationalData
            ? "Partial"
            : "Unavailable"
          : hasOperationalData
            ? "Live"
            : "Loading";
  const operationalLoading = !topology && !system && (topologyQuery.loading || systemQuery.loading);
  const operationalFailure = !topology && !system && topologyQuery.error && systemQuery.error;

  return (
    <DomainPageFrame>
      <Stack gap="3">
        <DomainHeader
          eyebrow="Operator overview"
          title="Fitz status"
          description="Actionable health across the selected Route Family."
          primaryAction={{
            busy: topologyQuery.refreshing || systemQuery.refreshing,
            disabled: topologyQuery.refreshing || systemQuery.refreshing,
            label: "Refresh overview",
            onPress: () => {
              void topologyQuery.refresh();
              void systemQuery.refresh();
            },
          }}
          status={{
            detail: overview.generatedAt
              ? `Updated ${formatRelativeTime(overview.generatedAt)}`
              : "Loading broker status signals.",
            label: refreshState,
            tone:
              topologyQuery.refreshing || systemQuery.refreshing
                ? "info"
                : topologyQuery.stale || systemQuery.stale || sourceUnavailable
                  ? "warning"
                  : overview.overall.tone,
          }}
        />
        <OperatorScopeStrip freshness={refreshState} />

        <Show when={operationalLoading}>
          <QueryLoadingState description="Loading overview status signals..." />
        </Show>

        <Show when={operationalFailure}>
          <QueryErrorState
            title="Unable to load overview"
            error={topologyQuery.error ?? systemQuery.error}
            onRetry={() => {
              void topologyQuery.refresh();
              void systemQuery.refresh();
            }}
          />
        </Show>

        <Show when={!operationalLoading && !operationalFailure}>
          <Show when={topologyQuery.error && !topology}>
            <Alert
              variant="warning"
              title="Topology signals unavailable"
              description={formatUnknownError(topologyQuery.error)}
            />
          </Show>
          <Show when={systemQuery.error && !system}>
            <Alert
              variant="warning"
              title="System counters unavailable"
              description={formatUnknownError(systemQuery.error)}
            />
          </Show>

          <OverviewStatusBand overview={overview} />
          <OverviewIssues overview={overview} />
          <DomainHealth overview={overview} />
          <BrokerVitals overview={overview} />
        </Show>
      </Stack>
    </DomainPageFrame>
  );
}
