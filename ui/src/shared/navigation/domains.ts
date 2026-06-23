import {
  DatabaseIcon,
  FileLockIcon,
  BoxesIcon,
  GaugeIcon,
  LayoutDashboardIcon,
  MessagesSquareIcon,
  NetworkIcon,
  DatabaseZapIcon,
  Rows3Icon,
  SettingsIcon,
  TimerResetIcon,
} from "@askrjs/lucide";

export type DomainSegment = "stream" | "kv" | "schedule" | "queue" | "lease" | "notice" | "rpc";
export type GenericResourceDomainSegment = Exclude<DomainSegment, "queue">;

export interface ResourceRouteScope {
  area: string;
  realm: string;
  resource: string;
}

export interface DomainLink {
  href: string;
  segment: DomainSegment;
  title: string;
  description: string;
  icon: typeof BoxesIcon;
}

export interface ShellLink {
  href: string;
  title: string;
  icon: typeof BoxesIcon;
}

export const RESOURCE_ROUTE_SHAPE = "{realm}/{area}/{resource}";

export function domainHref(segment: DomainSegment) {
  return `/${segment}`;
}

export function domainResourceRoutePath(segment: DomainSegment) {
  return `${domainHref(segment)}/${RESOURCE_ROUTE_SHAPE}`;
}

export function domainResourceHref(segment: DomainSegment, scope: ResourceRouteScope) {
  return `${domainHref(segment)}/${encodeURIComponent(scope.realm)}/${encodeURIComponent(
    scope.area,
  )}/${encodeURIComponent(scope.resource)}`;
}

export function domainScopeHref(segment: DomainSegment, scope: Partial<ResourceRouteScope>) {
  if (scope.realm && scope.area && scope.resource) {
    return domainResourceHref(segment, {
      area: scope.area,
      realm: scope.realm,
      resource: scope.resource,
    });
  }

  const query = new URLSearchParams();

  if (scope.realm) query.set("realm", scope.realm);
  if (scope.area) query.set("area", scope.area);
  if (scope.resource) query.set("resource", scope.resource);

  const queryString = query.toString();

  return queryString ? `${domainHref(segment)}?${queryString}` : domainHref(segment);
}

export const shellLinks: ShellLink[] = [
  {
    href: "/",
    title: "Overview",
    icon: LayoutDashboardIcon,
  },
  {
    href: "/diagnostics",
    title: "Diagnostics",
    icon: GaugeIcon,
  },
  {
    href: "/settings",
    title: "Settings",
    icon: SettingsIcon,
  },
];

export const domainLinks: DomainLink[] = [
  {
    href: "/stream",
    segment: "stream",
    title: "Stream",
    description: "Durable history, event exploration, and replay evidence.",
    icon: DatabaseZapIcon,
  },
  {
    href: "/kv",
    segment: "kv",
    title: "KV",
    description: "Current authoritative state, lookup, and validation workflows.",
    icon: DatabaseIcon,
  },
  {
    href: "/schedule",
    segment: "schedule",
    title: "Schedule",
    description: "Durable timing intent, timeline review, and execution health.",
    icon: TimerResetIcon,
  },
  {
    href: "/queue",
    segment: "queue",
    title: "Queue",
    description: "Durable work delivery, backlog, retries, and dead letters.",
    icon: Rows3Icon,
  },
  {
    href: "/lease",
    segment: "lease",
    title: "Lease",
    description: "Ephemeral ownership, contention, and lease health.",
    icon: FileLockIcon,
  },
  {
    href: "/notice",
    segment: "notice",
    title: "Notice",
    description: "Live ephemeral fanout, participants, and delivery pressure.",
    icon: MessagesSquareIcon,
  },
  {
    href: "/rpc",
    segment: "rpc",
    title: "RPC",
    description: "Live request/response flow, workers, failures, and latency.",
    icon: NetworkIcon,
  },
];

export const domainSegments = domainLinks.map((link) => link.segment);
export const genericResourceDomainSegments = domainSegments.filter(
  (segment): segment is GenericResourceDomainSegment => segment !== "queue",
);

export function isDomainSegment(value: string | undefined): value is DomainSegment {
  return domainSegments.some((segment) => segment === value);
}

export function isGenericResourceDomainSegment(
  value: string | undefined,
): value is GenericResourceDomainSegment {
  return genericResourceDomainSegments.some((segment) => segment === value);
}

export function domainTitleForSegment(segment: DomainSegment) {
  return domainLinks.find((link) => link.segment === segment)?.title ?? segment;
}
