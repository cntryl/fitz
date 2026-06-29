import { group, lazy, route, type RouteHandler } from "@askrjs/askr/router";
import Layout from "./_layout";
import {
  domainLinks,
  domainResourceRoutePath,
  legacyDomainResourceRoutePath,
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

export function registerAppRoutes() {
  group({ layout: Layout, auth: true }, () => {
    route("/", Home);
    route("/admin", Home);
    route("/admin/{family}", Home);
    route("/sessions", SessionsPage);
    route("/admin/metrics", MetricsPage);
    route("/admin/{family}/sessions", SessionsPage);
    route("/admin/{family}/metrics", MetricsPage);
    route("/diagnostics", DiagnosticsPage);
    route("/admin/{family}/diagnostics", DiagnosticsPage);
    route("/settings", SettingsPage);
    route("/admin/{family}/settings", SettingsPage);

    for (const link of domainLinks) {
      route(`/${link.segment}`, domainPageBySegment[link.segment]);
      route(`/admin/{family}/${link.segment}`, domainPageBySegment[link.segment]);
      if (link.segment === "queue") {
        route("/queue/{realm}", QueuePage);
        route("/queue/{realm}/{area}", QueuePage);
        route("/admin/{family}/queue/{realm}", QueuePage);
        route("/admin/{family}/queue/{realm}/{area}", QueuePage);
      }
      if (link.segment === "kv") {
        route("/kv/{realm}", KvPage);
        route("/kv/{realm}/{area}", KvPage);
        route("/admin/{family}/kv/{realm}", KvPage);
        route("/admin/{family}/kv/{realm}/{area}", KvPage);
      }

      if (link.segment === "lease") {
        route("/lease/{realm}", LeasePage);
        route("/lease/{realm}/{area}", LeasePage);
        route("/admin/{family}/lease/{realm}", LeasePage);
        route("/admin/{family}/lease/{realm}/{area}", LeasePage);
      }
      if (link.segment === "notice") {
        route("/notice/{realm}", NoticePage);
        route("/notice/{realm}/{area}", NoticePage);
        route("/notice/{realm}/{area}/{resource}", NoticePage);
        route("/notice/{realm}/{area}/{resource}/{operation}", NoticePage);
        route("/admin/{family}/notice/{realm}", NoticePage);
        route("/admin/{family}/notice/{realm}/{area}", NoticePage);
        route("/admin/{family}/notice/{realm}/{area}/{resource}", NoticePage);
        route("/admin/{family}/notice/{realm}/{area}/{resource}/{operation}", NoticePage);
      }
      if (link.segment === "rpc") {
        route("/rpc/{realm}", RpcPage);
        route("/rpc/{realm}/{area}", RpcPage);
        route("/rpc/{realm}/{area}/{resource}/{operation}", RpcOperationPage);
        route("/admin/{family}/rpc/{realm}", RpcPage);
        route("/admin/{family}/rpc/{realm}/{area}", RpcPage);
        route("/admin/{family}/rpc/{realm}/{area}/{resource}/{operation}", RpcOperationPage);
      }
      if (link.segment === "stream") {
        route("/stream/{realm}", StreamPage);
        route("/stream/{realm}/{area}", StreamPage);
        route("/admin/{family}/stream/{realm}", StreamPage);
        route("/admin/{family}/stream/{realm}/{area}", StreamPage);
      }
      if (link.segment === "schedule") {
        route("/schedule/{realm}", SchedulePage);
        route("/schedule/{realm}/{area}", SchedulePage);
        route("/admin/{family}/schedule/{realm}", SchedulePage);
        route("/admin/{family}/schedule/{realm}/{area}", SchedulePage);
      }
      route(legacyDomainResourceRoutePath(link.segment), resourcePageBySegment[link.segment]);
      route(domainResourceRoutePath(link.segment), resourcePageBySegment[link.segment]);
    }
  });
}
