import {
  DatabaseIcon,
  FileLockIcon,
  BoxesIcon,
  ChartLineIcon,
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

export interface FitzRouteDisplayScope {
  area?: string | null;
  operation?: string | null;
  realm?: string | null;
  resource?: string | null;
}

export interface DomainLink {
  href: string;
  segment: DomainSegment;
  title: string;
  description: string;
  icon: typeof BoxesIcon;
}

export type DomainRouteScopeLevel = "realm" | "area" | "resource" | "operation";

export interface DomainRouteMetadata {
  scopeLevels: readonly DomainRouteScopeLevel[];
}

export interface DomainDefinition extends DomainLink {
  routes: DomainRouteMetadata;
}

export interface ShellLink {
  href: string;
  title: string;
  icon: typeof BoxesIcon;
}

export const DEFAULT_ROUTE_FAMILY_SEGMENT = "1";
export const LEGACY_ALL_ROUTE_FAMILY_SEGMENT = "all";
export const RESOURCE_ROUTE_SHAPE = "{realm}/{area}/{resource}";
const routeFamilyPattern = /^\d+$/;
const domainSegmentValues = [
  "stream",
  "kv",
  "schedule",
  "queue",
  "lease",
  "notice",
  "rpc",
] as const satisfies readonly DomainSegment[];
const adminContentSegments = new Set<string>([
  "diagnostics",
  "metrics",
  "settings",
  "sessions",
  ...domainSegmentValues,
]);

export function isRouteFamilyPathSegment(value: string | undefined) {
  return routeFamilyPattern.test(value ?? "");
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

    if (parts[1] === LEGACY_ALL_ROUTE_FAMILY_SEGMENT) {
      return adminChildHref(parts.slice(2).join("/"), family);
    }

    if (adminContentSegments.has(parts[1] ?? "")) {
      return adminChildHref(parts.slice(1).join("/"), family);
    }

    if (adminContentSegments.has(parts[2] ?? "")) {
      return adminChildHref(parts.slice(2).join("/"), family);
    }

    return adminHref(family);
  }

  return adminChildHref(parts.join("/"), family);
}

export function contentPathFromRouteFamilyPath(path: string) {
  const parts = path.split("/").filter(Boolean);

  if (parts[0] !== "admin") {
    return path === "/" ? "/" : `/${parts.join("/")}`;
  }

  if (isRouteFamilyPathSegment(parts[1]) || parts[1] === LEGACY_ALL_ROUTE_FAMILY_SEGMENT) {
    const child = parts.slice(2).join("/");

    return child ? `/${child}` : "/";
  }

  if (adminContentSegments.has(parts[1] ?? "")) {
    return `/${parts.slice(1).join("/")}`;
  }

  if (adminContentSegments.has(parts[2] ?? "")) {
    return `/${parts.slice(2).join("/")}`;
  }

  return "/";
}

export function domainHref(segment: DomainSegment, family = currentRouteFamilySegment()) {
  return adminChildHref(segment, family);
}

export function domainResourceRoutePath(segment: DomainSegment) {
  return `/admin/{family}/${segment}/${RESOURCE_ROUTE_SHAPE}`;
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

export function formatFitzRoute(domain: string, scope: FitzRouteDisplayScope) {
  const routeParts = [scope.realm, scope.area, scope.resource, scope.operation].filter(
    (part): part is string => typeof part === "string" && part.length > 0,
  );

  return `${domain}://${routeParts.join("/")}`;
}

export function domainScopeHref(
  segment: DomainSegment,
  scope: Partial<ResourceRouteScope> = {},
  family = currentRouteFamilySegment(),
) {
  const scopeLevels: readonly DomainRouteScopeLevel[] =
    domainDefinitions[segment].routes.scopeLevels;
  const hasScopeLevel = (level: DomainRouteScopeLevel) => scopeLevels.includes(level);

  if (hasScopeLevel("resource") && scope.realm && scope.area && scope.resource) {
    const base = domainResourceHref(
      segment,
      {
        area: scope.area,
        realm: scope.realm,
        resource: scope.resource,
      },
      family,
    );

    return hasScopeLevel("operation") && scope.operation !== undefined
      ? `${base}/${encodeURIComponent(scope.operation)}`
      : base;
  }

  if (hasScopeLevel("area") && scope.realm && scope.area) {
    return `${domainHref(segment, family)}/${encodeURIComponent(scope.realm)}/${encodeURIComponent(
      scope.area,
    )}`;
  }

  if (hasScopeLevel("realm") && scope.realm) {
    return `${domainHref(segment, family)}/${encodeURIComponent(scope.realm)}`;
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
    href: "/",
    title: "Overview",
    icon: LayoutDashboardIcon,
  },
  {
    href: "/sessions",
    title: "Sessions",
    icon: Rows3Icon,
  },
  {
    href: "/diagnostics",
    title: "Diagnostics",
    icon: GaugeIcon,
  },
  {
    href: "/metrics",
    title: "Metrics",
    icon: ChartLineIcon,
  },
  {
    href: "/settings",
    title: "Workspace & account",
    icon: SettingsIcon,
  },
];

export const domainDefinitions = {
  stream: {
    href: "/stream",
    segment: "stream",
    title: "Stream",
    description: "Durable history, event exploration, and replay evidence.",
    icon: DatabaseZapIcon,
    routes: {
      scopeLevels: ["realm", "area", "resource"],
    },
  },
  kv: {
    href: "/kv",
    segment: "kv",
    title: "KV",
    description: "Current authoritative state, lookup, and validation workflows.",
    icon: DatabaseIcon,
    routes: {
      scopeLevels: ["realm", "area", "resource"],
    },
  },
  schedule: {
    href: "/schedule",
    segment: "schedule",
    title: "Schedule",
    description: "Durable timing intent, timeline review, and handoff health.",
    icon: TimerResetIcon,
    routes: {
      scopeLevels: ["realm", "area", "resource"],
    },
  },
  queue: {
    href: "/queue",
    segment: "queue",
    title: "Queue",
    description: "Durable work delivery, backlog, retries, and dead letters.",
    icon: Rows3Icon,
    routes: {
      scopeLevels: ["realm", "area", "resource"],
    },
  },
  lease: {
    href: "/lease",
    segment: "lease",
    title: "Lease",
    description: "Ephemeral ownership, contention, and lease health.",
    icon: FileLockIcon,
    routes: {
      scopeLevels: ["realm", "area", "resource"],
    },
  },
  notice: {
    href: "/notice",
    segment: "notice",
    title: "Notice",
    description: "Live ephemeral fanout, participants, and delivery pressure.",
    icon: MessagesSquareIcon,
    routes: {
      scopeLevels: ["realm", "area", "resource", "operation"],
    },
  },
  rpc: {
    href: "/rpc",
    segment: "rpc",
    title: "RPC",
    description: "Live request/response flow, workers, failures, and latency.",
    icon: NetworkIcon,
    routes: {
      scopeLevels: ["realm", "area", "resource", "operation"],
    },
  },
} satisfies Record<DomainSegment, DomainDefinition>;

export const domainLinks: DomainLink[] = domainSegmentValues.map((segment): DomainLink => {
  const { routes: _routes, ...link } = domainDefinitions[segment];

  return link;
});

export const domainSegments = [...domainSegmentValues];
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
