import { group, lazy, route, type AuthRequirement, type RouteHandler } from "@askrjs/askr/router";
import Layout from "./_layout";
import RouteFamilySelectorPage from "./route-family";
import {
  domainDefinitions,
  domainLinks,
  domainResourceRoutePath,
  type DomainSegment,
} from "@/shared/navigation/domains";

const DiagnosticsPage = lazy(() => import("./diagnostics"));
const Home = lazy(() => import("./home"));
const KvPage = lazy(() => import("./kv"));
const KvResourcePage = lazy(() => import("./kv-resource"));
const LeasePage = lazy(() => import("./lease"));
const LeaseResourcePage = lazy(() => import("./lease-resource"));
const MetricsPage = lazy(() => import("./metrics"));
const NoticePage = lazy(() => import("./notice"));
const QueuePage = lazy(() => import("./queue"));
const QueueResourcePage = lazy(() => import("./queue-resource"));
const RpcPage = lazy(() => import("./rpc"));
const RpcOperationPage = lazy(() => import("./rpc-operation"));
const RpcResourcePage = lazy(() => import("./rpc-resource"));
const SchedulePage = lazy(() => import("./schedule"));
const ScheduleResourcePage = lazy(() => import("./schedule-resource"));
const SessionsPage = lazy(() => import("./sessions"));
const SettingsPage = lazy(() => import("./settings"));
const StreamPage = lazy(() => import("./stream"));
const StreamResourcePage = lazy(() => import("./stream-resource"));

const requireAuthenticated: AuthRequirement = (auth) =>
  auth.authenticated ? { allowed: true } : { allowed: false, reason: "unauthenticated" };

const domainPageBySegment: Record<DomainSegment, RouteHandler> = {
  kv: KvPage,
  lease: LeasePage,
  notice: NoticePage,
  queue: QueuePage,
  rpc: RpcPage,
  schedule: SchedulePage,
  stream: StreamPage,
};

const resourcePageBySegment: Record<DomainSegment, RouteHandler> = {
  kv: KvResourcePage,
  lease: LeaseResourcePage,
  notice: NoticePage,
  queue: QueueResourcePage,
  rpc: RpcResourcePage,
  schedule: ScheduleResourcePage,
  stream: StreamResourcePage,
};

const operationPageBySegment: Partial<Record<DomainSegment, RouteHandler>> = {
  notice: NoticePage,
  rpc: RpcOperationPage,
};

function hasRouteScope(segment: DomainSegment, scope: "realm" | "area" | "operation") {
  const scopeLevels: readonly string[] = domainDefinitions[segment].routes.scopeLevels;

  return scopeLevels.includes(scope);
}

export function registerAppRoutes() {
  group({ layout: Layout, auth: requireAuthenticated }, () => {
    route("/", RouteFamilySelectorPage);
    route("/admin", RouteFamilySelectorPage);
    route("/admin/metrics", RouteFamilySelectorPage);
    route("/admin/{family}", Home);
    route("/sessions", RouteFamilySelectorPage);
    route("/admin/{family}/sessions", SessionsPage);
    route("/admin/{family}/metrics", MetricsPage);
    route("/diagnostics", RouteFamilySelectorPage);
    route("/admin/{family}/diagnostics", DiagnosticsPage);
    route("/settings", RouteFamilySelectorPage);
    route("/admin/{family}/settings", SettingsPage);

    for (const link of domainLinks) {
      route(`/${link.segment}`, RouteFamilySelectorPage);
      route(`/admin/{family}/${link.segment}`, domainPageBySegment[link.segment]);

      if (hasRouteScope(link.segment, "realm")) {
        route(`/${link.segment}/{realm}`, RouteFamilySelectorPage);
        route(`/admin/{family}/${link.segment}/{realm}`, domainPageBySegment[link.segment]);
      }

      if (hasRouteScope(link.segment, "area")) {
        route(`/${link.segment}/{realm}/{area}`, RouteFamilySelectorPage);
        route(`/admin/{family}/${link.segment}/{realm}/{area}`, domainPageBySegment[link.segment]);
      }

      if (hasRouteScope(link.segment, "operation")) {
        const operationPage =
          operationPageBySegment[link.segment] ?? domainPageBySegment[link.segment];

        route(
          `/admin/{family}/${link.segment}/{realm}/{area}/{resource}/{operation}`,
          operationPage,
        );
      }

      route(domainResourceRoutePath(link.segment), resourcePageBySegment[link.segment]);
    }
  });
}
