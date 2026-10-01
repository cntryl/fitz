import type { DiagnosticHotspot, DiagnosticSeverity, IncidentSummary } from "@/adapters";
import type { SystemOverview } from "@/features/system/system-models";
import type { MessagingTopologyOverview, TopologyLane } from "@/features/topology/topology-models";
import { hotspotHref, humanizeSeconds, scopeText } from "@/features/topology/topology-view";
import { overviewDomainIssueDescriptors } from "@/features/overview/overview-domain-rules";
import { formatNumber } from "@/shared/format";
import { adminChildHref, domainLinks, type DomainSegment } from "@/shared/navigation/domains";

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

export interface OverviewVital {
  caption?: string;
  label: string;
  value: string;
}

export interface OverviewStatus {
  complete: boolean;
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

/** Broker-reported labels arrive lowercase; issue titles read as sentences. */
function sentenceCase(text: string) {
  return text.charAt(0).toUpperCase() + text.slice(1);
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
    title: sentenceCase(
      hotspot.likely_bottleneck ??
        `${domainLinkBySegment.get(domain!)?.title ?? hotspot.domain} pressure`,
    ),
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

/**
 * A lane issue only says "this domain is under pressure". Once a hotspot or system
 * signal names the same domain at the same or higher severity, the lane repeats it.
 */
function withoutRedundantLaneIssues(issues: readonly OverviewIssue[]) {
  return issues.filter(
    (issue) =>
      !issue.id.startsWith("lane:") ||
      !issues.some(
        (other) =>
          other !== issue &&
          !other.id.startsWith("lane:") &&
          other.domain === issue.domain &&
          severityRank[other.severity] >= severityRank[issue.severity],
      ),
  );
}

function issueSort(left: OverviewIssue, right: OverviewIssue) {
  return (
    severityRank[right.severity] - severityRank[left.severity] ||
    left.title.localeCompare(right.title)
  );
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
      value: broker ? formatNumber(broker.sessions) : "--",
    },
    {
      caption: "Client connections",
      label: "Connections",
      value: broker ? formatNumber(broker.connections) : "--",
    },
    {
      caption: "Broker message rate",
      label: "Messages/sec",
      value: broker ? broker.messagesPerSecond.toFixed(2) : "--",
    },
    {
      caption: "Observed namespaces",
      label: "Realms",
      value: broker ? formatNumber(broker.realms.length) : "--",
    },
    {
      caption: "Current process uptime",
      label: "Uptime",
      value: broker ? humanizeSeconds(broker.uptimeSeconds) : "--",
    },
    {
      caption: "Router backpressure",
      label: "Router pressure",
      value: topology ? formatNumber(topology.broker.routerBackpressureTotal) : "--",
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

  const issues = withoutRedundantLaneIssues(Array.from(issueMap.values())).sort(issueSort);
  const topIssue = issues[0];
  const complete = Boolean(system && topology);
  const overall = topIssue
    ? {
        description: `${issues.length} actionable signal${issues.length === 1 ? "" : "s"} detected. Start with ${topIssue.scope}.`,
        label: topIssue.severity,
        title: topIssue.title,
        tone: topIssue.tone,
      }
    : !complete
      ? {
          description: `${system ? "System counters are available" : "System counters are unavailable"}; ${topology ? "topology signals are available" : "topology signals are unavailable"}. Health cannot be confirmed from a partial snapshot.`,
          label: "Partial",
          title: "Incomplete snapshot",
          tone: "warning" as const,
        }
      : {
          description:
            "No actionable pressure, failure, backlog, or contention signals are active in the current snapshot.",
          label: "Healthy",
          title: "No active issues",
          tone: "success" as const,
        };

  return {
    complete,
    generatedAt: topology?.generatedAt ?? system?.fetchedAt,
    issues,
    overall,
    vitals: brokerVitals(topology, system),
  };
}
