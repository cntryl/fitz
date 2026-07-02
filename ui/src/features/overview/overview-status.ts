import type { DiagnosticHotspot, DiagnosticSeverity, IncidentSummary } from "@/adapters";
import type { SystemOverview } from "@/features/system/system-models";
import type {
  MessagingTopologyOverview,
  TopologyDomain,
  TopologyLane,
  TopologyState,
} from "@/features/topology/topology-models";
import { hotspotHref, humanizeSeconds, scopeText } from "@/features/topology/topology-view";
import {
  overviewDomainIssueDescriptors,
  overviewDomainSignal,
} from "@/features/overview/overview-domain-rules";
import { formatNumber } from "@/shared/format";
import {
  adminChildHref,
  domainHref,
  domainLinks,
  type DomainSegment,
} from "@/shared/navigation/domains";

export type OverviewTone = "danger" | "info" | "success" | "warning";

export interface OverviewIssue {
  action: string;
  description: string;
  domain?: DomainSegment;
  href: string;
  id: string;
  scope: string;
  severity: DiagnosticSeverity;
  title: string;
  tone: OverviewTone;
}

export interface OverviewDomainStatus {
  description: string;
  href: string;
  issueCount: number;
  signal: string;
  state: string;
  title: string;
  tone: OverviewTone;
}

export interface OverviewVital {
  caption?: string;
  label: string;
  value: string;
}

export interface OverviewStatus {
  domains: OverviewDomainStatus[];
  generatedAt?: string;
  issues: OverviewIssue[];
  overall: {
    description: string;
    label: string;
    title: string;
    tone: OverviewTone;
  };
  vitals: OverviewVital[];
}

const severityRank: Record<DiagnosticSeverity, number> = {
  critical: 4,
  high: 3,
  informational: 0,
  low: 1,
  medium: 2,
};

const domainLinkBySegment = new Map(domainLinks.map((link) => [link.segment, link]));

function isDomainSegment(value: string | null | undefined): value is DomainSegment {
  return domainLinks.some((link) => link.segment === value);
}

function severityTone(severity: DiagnosticSeverity): OverviewTone {
  if (severity === "critical" || severity === "high") return "danger";
  if (severity === "medium") return "warning";
  if (severity === "low") return "info";
  return "success";
}

function stateLabel(state: TopologyState | undefined, issueCount: number) {
  if (issueCount > 0) return issueCount === 1 ? "Issue" : `${issueCount} issues`;
  if (state === "flowing") return "Live";
  if (state === "quiet") return "Quiet";
  if (state === "blocked" || state === "pressure") return "Watch";
  return "Unknown";
}

function actionForHref(href: string, domain?: DomainSegment) {
  if (href.endsWith("/diagnostics")) return "Open diagnostics";
  if (domain) return `Open ${domainLinkBySegment.get(domain)?.title ?? domain}`;
  return "Open scope";
}

function addIssue(issues: Map<string, OverviewIssue>, issue: OverviewIssue | null) {
  if (!issue) return;

  const existing = issues.get(issue.id);
  if (!existing || severityRank[issue.severity] > severityRank[existing.severity]) {
    issues.set(issue.id, issue);
  }
}

function actionableSeverity(severity: DiagnosticSeverity) {
  return severityRank[severity] >= severityRank.medium;
}

function incidentIssue(summary: IncidentSummary | undefined): OverviewIssue | null {
  if (!summary || !actionableSeverity(summary.severity) || summary.status === "healthy") {
    return null;
  }

  return {
    action: "Open diagnostics",
    description: summary.explanation,
    href: adminChildHref("diagnostics"),
    id: "incident-summary",
    scope: "Broker",
    severity: summary.severity,
    title: summary.title,
    tone: severityTone(summary.severity),
  };
}

function hotspotIssue(hotspot: DiagnosticHotspot): OverviewIssue | null {
  if (!actionableSeverity(hotspot.severity) || hotspot.current_stage === "healthy") {
    return null;
  }

  const domain = isDomainSegment(hotspot.domain) ? hotspot.domain : undefined;
  const href = hotspotHref(hotspot) ?? (domain ? domainLinkBySegment.get(domain)?.href : undefined);

  return {
    action: actionForHref(href ?? "/diagnostics", domain),
    description:
      hotspot.explanation_hints[0] ??
      hotspot.likely_bottleneck ??
      "Actionable diagnostic pressure was reported for this scope.",
    domain,
    href: href ?? "/diagnostics",
    id: `hotspot:${hotspot.domain}:${hotspot.realm ?? "*"}:${hotspot.area ?? "*"}:${hotspot.resource ?? "*"}:${hotspot.current_stage}`,
    scope: scopeText(hotspot) || "Broker",
    severity: hotspot.severity,
    title:
      hotspot.likely_bottleneck ??
      `${domainLinkBySegment.get(domain!)?.title ?? hotspot.domain} pressure`,
    tone: severityTone(hotspot.severity),
  };
}

function laneIssue(lane: TopologyLane): OverviewIssue | null {
  if (
    lane.state !== "blocked" &&
    !(lane.state === "pressure" && actionableSeverity(lane.diagnostics.severity))
  ) {
    return null;
  }

  if (!actionableSeverity(lane.diagnostics.severity)) {
    return null;
  }

  const domain = lane.id;
  const domainTitle = domainLinkBySegment.get(domain)?.title ?? lane.title;

  return {
    action: actionForHref(lane.href, domain),
    description:
      lane.diagnostics.explanation_hints[0] ??
      lane.diagnostics.likely_bottleneck ??
      "Domain diagnostics report actionable pressure.",
    domain,
    href: lane.href,
    id: `lane:${lane.id}:${lane.diagnostics.current_stage}`,
    scope: domainTitle,
    severity: lane.diagnostics.severity,
    title: `${domainTitle} ${lane.state}`,
    tone: severityTone(lane.diagnostics.severity),
  };
}

function systemIssue(
  id: string,
  domain: DomainSegment,
  severity: DiagnosticSeverity,
  title: string,
  description: string,
): OverviewIssue {
  const href = domainLinkBySegment.get(domain)?.href ?? "/diagnostics";

  return {
    action: actionForHref(href, domain),
    description,
    domain,
    href,
    id: `system:${id}`,
    scope: domainLinkBySegment.get(domain)?.title ?? domain,
    severity,
    title,
    tone: severityTone(severity),
  };
}

function systemIssues(system: SystemOverview | null | undefined) {
  const issues = new Map<string, OverviewIssue>();
  if (!system) return issues;

  for (const issue of overviewDomainIssueDescriptors(system.domains)) {
    addIssue(
      issues,
      systemIssue(issue.id, issue.domain, issue.severity, issue.title, issue.description),
    );
  }

  return issues;
}

function issueSort(left: OverviewIssue, right: OverviewIssue) {
  return (
    severityRank[right.severity] - severityRank[left.severity] ||
    left.title.localeCompare(right.title)
  );
}

function laneSignal(lane: TopologyLane | undefined) {
  if (!lane) return null;

  const primary = lane.counters
    .filter((counter) => counter.value > 0)
    .slice(0, 2)
    .map((counter) => `${counter.label} ${formatNumber(counter.value)}`);

  if (primary.length > 0) return primary.join(" / ");
  if (lane.activityPerSecond > 0) return `${lane.activityPerSecond.toFixed(2)} act/sec`;
  return null;
}

function domainStatuses(
  topology: MessagingTopologyOverview | null | undefined,
  system: SystemOverview | null | undefined,
  issues: OverviewIssue[],
): OverviewDomainStatus[] {
  const laneById = new Map<TopologyDomain, TopologyLane>(
    topology?.lanes.map((lane) => [lane.id, lane]) ?? [],
  );

  return domainLinks.map((link) => {
    const lane = laneById.get(link.segment);
    const domainIssues = issues.filter((issue) => issue.domain === link.segment);
    const topIssue = domainIssues[0];
    const tone = topIssue
      ? topIssue.tone
      : lane?.state === "flowing"
        ? "success"
        : lane?.state === "quiet"
          ? "info"
          : "info";

    return {
      description: link.description,
      href: domainHref(link.segment),
      issueCount: domainIssues.length,
      signal:
        topIssue?.description ??
        laneSignal(lane) ??
        overviewDomainSignal(link.segment, system?.domains) ??
        link.description,
      state: stateLabel(lane?.state, domainIssues.length),
      title: link.title,
      tone,
    };
  });
}

function brokerVitals(
  topology: MessagingTopologyOverview | null | undefined,
  system: SystemOverview | null | undefined,
): OverviewVital[] {
  const broker = topology?.broker ?? system?.broker;

  return [
    {
      caption: "Current live sessions",
      label: "Sessions",
      value: broker ? formatNumber(broker.sessions) : "n/a",
    },
    {
      caption: "Client connections",
      label: "Connections",
      value: broker ? formatNumber(broker.connections) : "n/a",
    },
    {
      caption: "Broker message rate",
      label: "Messages/sec",
      value: broker ? broker.messagesPerSecond.toFixed(2) : "n/a",
    },
    {
      caption: "Observed namespaces",
      label: "Realms",
      value: broker ? formatNumber(broker.realms.length) : "n/a",
    },
    {
      caption: "Current process uptime",
      label: "Uptime",
      value: broker ? humanizeSeconds(broker.uptimeSeconds) : "n/a",
    },
    {
      caption: "Router backpressure",
      label: "Router pressure",
      value: topology ? formatNumber(topology.broker.routerBackpressureTotal) : "n/a",
    },
  ];
}

export function buildOverviewStatus({
  system,
  topology,
}: {
  system?: SystemOverview | null;
  topology?: MessagingTopologyOverview | null;
}): OverviewStatus {
  const issueMap = systemIssues(system);

  addIssue(
    issueMap,
    incidentIssue(topology?.diagnostics.incident_summary ?? system?.diagnostics.incident_summary),
  );
  for (const hotspot of topology?.diagnostics.hotspots ?? system?.diagnostics.hotspots ?? []) {
    addIssue(issueMap, hotspotIssue(hotspot));
  }
  for (const lane of topology?.lanes ?? []) {
    addIssue(issueMap, laneIssue(lane));
  }

  const issues = Array.from(issueMap.values()).sort(issueSort);
  const topIssue = issues[0];
  const overall = topIssue
    ? {
        description: `${issues.length} actionable signal${issues.length === 1 ? "" : "s"} detected. Start with ${topIssue.scope}.`,
        label: topIssue.severity,
        title: topIssue.title,
        tone: topIssue.tone,
      }
    : {
        description:
          "No actionable pressure, failure, backlog, or contention signals are active in the current snapshot.",
        label: "Healthy",
        title: "No active issues",
        tone: "success" as const,
      };

  return {
    domains: domainStatuses(topology, system, issues),
    generatedAt: topology?.generatedAt ?? system?.fetchedAt,
    issues,
    overall,
    vitals: brokerVitals(topology, system),
  };
}
