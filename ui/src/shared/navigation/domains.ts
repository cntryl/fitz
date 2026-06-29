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
  operation?: string;
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

export const DEFAULT_ROUTE_FAMILY_SEGMENT = "all";
export const RESOURCE_ROUTE_SHAPE = "{realm}/{area}/{resource}";

export function isRouteFamilyPathSegment(value: string | undefined) {
  return value === DEFAULT_ROUTE_FAMILY_SEGMENT || /^\d+$/.test(value ?? "");
}

export function routeFamilyFromPath(path: string) {
  const parts = path.split("/").filter(Boolean);

  if (parts[0] !== "admin" || !isRouteFamilyPathSegment(parts[1])) {
    return null;
  }

  return decodeURIComponent(parts[1]);
}

export function currentRouteFamilySegment() {
  if (typeof window === "undefined") {
    return DEFAULT_ROUTE_FAMILY_SEGMENT;
  }

  return routeFamilyFromPath(window.location.pathname) ?? DEFAULT_ROUTE_FAMILY_SEGMENT;
}

export function apiRouteFamilySegment(family?: number | string | null) {
  return family === undefined || family === null ? currentRouteFamilySegment() : String(family);
}

export function adminHref(family = currentRouteFamilySegment()) {
  return `/admin/${encodeURIComponent(family || DEFAULT_ROUTE_FAMILY_SEGMENT)}`;
}

export function adminChildHref(path: string, family = currentRouteFamilySegment()) {
  const child = path.replace(/^\/+/, "");

  return child ? `${adminHref(family)}/${child}` : adminHref(family);
}

export function pathWithRouteFamily(path: string, family: string) {
  if (path === "/" || path === "/admin") {
    return adminHref(family);
  }

  const parts = path.split("/").filter(Boolean);

  if (parts[0] === "admin") {
    if (isRouteFamilyPathSegment(parts[1])) {
      return `/${["admin", encodeURIComponent(family), ...parts.slice(2)].join("/")}`;
    }

    return adminChildHref(parts.slice(1).join("/"), family);
  }

  return adminChildHref(parts.join("/"), family);
}

export function domainHref(segment: DomainSegment, family = currentRouteFamilySegment()) {
  return adminChildHref(segment, family);
}

export function domainResourceRoutePath(segment: DomainSegment) {
  return `/admin/{family}/${segment}/${RESOURCE_ROUTE_SHAPE}`;
}

export function legacyDomainResourceRoutePath(segment: DomainSegment) {
  return `/${segment}/${RESOURCE_ROUTE_SHAPE}`;
}

export function domainResourceHref(
  segment: DomainSegment,
  scope: ResourceRouteScope,
  family = currentRouteFamilySegment(),
) {
  return `${domainHref(segment, family)}/${encodeURIComponent(scope.realm)}/${encodeURIComponent(
    scope.area,
  )}/${encodeURIComponent(scope.resource)}`;
}

export function domainScopeHref(
  segment: DomainSegment,
  scope: Partial<ResourceRouteScope> = {},
  family = currentRouteFamilySegment(),
) {
  if (segment === "notice" || segment === "rpc") {
    if (scope.realm && scope.area && scope.resource) {
      const base = domainResourceHref(
        segment,
        {
          area: scope.area,
          realm: scope.realm,
          resource: scope.resource,
        },
        family,
      );

      return scope.operation === undefined
        ? base
        : `${base}/${encodeURIComponent(scope.operation)}`;
    }

    if (scope.realm && scope.area) {
      return `${domainHref(segment, family)}/${encodeURIComponent(scope.realm)}/${encodeURIComponent(scope.area)}`;
    }

    if (scope.realm) {
      return `${domainHref(segment, family)}/${encodeURIComponent(scope.realm)}`;
    }
  }

  if (
    segment === "queue" ||
    segment === "kv" ||
    segment === "lease" ||
    segment === "stream" ||
    segment === "schedule"
  ) {
    if (scope.realm && scope.area && scope.resource) {
      return domainResourceHref(
        segment,
        {
          area: scope.area,
          realm: scope.realm,
          resource: scope.resource,
        },
        family,
      );
    }

    if (scope.realm && scope.area) {
      return `${domainHref(segment, family)}/${encodeURIComponent(scope.realm)}/${encodeURIComponent(
        scope.area,
      )}`;
    }

    if (scope.realm) {
      return `${domainHref(segment, family)}/${encodeURIComponent(scope.realm)}`;
    }
  }

  if (scope.realm && scope.area && scope.resource) {
    return domainResourceHref(
      segment,
      {
        area: scope.area,
        realm: scope.realm,
        resource: scope.resource,
      },
      family,
    );
  }

  const query = new URLSearchParams();

  if (scope.realm) query.set("realm", scope.realm);
  if (scope.area) query.set("area", scope.area);
  if (scope.resource) query.set("resource", scope.resource);

  const queryString = query.toString();

  return queryString
    ? `${domainHref(segment, family)}?${queryString}`
    : domainHref(segment, family);
}

export const shellLinks: ShellLink[] = [
  {
    href: adminHref(DEFAULT_ROUTE_FAMILY_SEGMENT),
    title: "Overview",
    icon: LayoutDashboardIcon,
  },
  {
    href: adminChildHref("diagnostics", DEFAULT_ROUTE_FAMILY_SEGMENT),
    title: "Diagnostics",
    icon: GaugeIcon,
  },
  {
    href: adminChildHref("settings", DEFAULT_ROUTE_FAMILY_SEGMENT),
    title: "Settings",
    icon: SettingsIcon,
  },
];

export const domainLinks: DomainLink[] = [
  {
    href: domainHref("stream", DEFAULT_ROUTE_FAMILY_SEGMENT),
    segment: "stream",
    title: "Stream",
    description: "Durable history, event exploration, and replay evidence.",
    icon: DatabaseZapIcon,
  },
  {
    href: domainHref("kv", DEFAULT_ROUTE_FAMILY_SEGMENT),
    segment: "kv",
    title: "KV",
    description: "Current authoritative state, lookup, and validation workflows.",
    icon: DatabaseIcon,
  },
  {
    href: domainHref("schedule", DEFAULT_ROUTE_FAMILY_SEGMENT),
    segment: "schedule",
    title: "Schedule",
    description: "Durable timing intent, timeline review, and execution health.",
    icon: TimerResetIcon,
  },
  {
    href: domainHref("queue", DEFAULT_ROUTE_FAMILY_SEGMENT),
    segment: "queue",
    title: "Queue",
    description: "Durable work delivery, backlog, retries, and dead letters.",
    icon: Rows3Icon,
  },
  {
    href: domainHref("lease", DEFAULT_ROUTE_FAMILY_SEGMENT),
    segment: "lease",
    title: "Lease",
    description: "Ephemeral ownership, contention, and lease health.",
    icon: FileLockIcon,
  },
  {
    href: domainHref("notice", DEFAULT_ROUTE_FAMILY_SEGMENT),
    segment: "notice",
    title: "Notice",
    description: "Live ephemeral fanout, participants, and delivery pressure.",
    icon: MessagesSquareIcon,
  },
  {
    href: domainHref("rpc", DEFAULT_ROUTE_FAMILY_SEGMENT),
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
