import { group, lazy, route, type RouteHandler } from "@askrjs/askr/router";
import Layout from "./_layout";
import {
  domainLinks,
  domainResourceRoutePath,
  type DomainSegment,
} from "@/shared/navigation/domains";

const DiagnosticsPage = lazy(() => import("./diagnostics"));
const Home = lazy(() => import("./home"));
const KvPage = lazy(() => import("./kv"));
const LeasePage = lazy(() => import("./lease"));
const MetricsPage = lazy(() => import("./metrics"));
const NoticePage = lazy(() => import("./notice"));
const QueuePage = lazy(() => import("./queue"));
const QueueResourcePage = lazy(() => import("./queue-resource"));
const ResourceDetailPage = lazy(() => import("./resource-detail"));
const RpcPage = lazy(() => import("./rpc"));
const SchedulePage = lazy(() => import("./schedule"));
const SessionsPage = lazy(() => import("./sessions"));
const SettingsPage = lazy(() => import("./settings"));
const StreamPage = lazy(() => import("./stream"));

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
  kv: ResourceDetailPage,
  lease: ResourceDetailPage,
  notice: ResourceDetailPage,
  queue: QueueResourcePage,
  rpc: ResourceDetailPage,
  schedule: ResourceDetailPage,
  stream: ResourceDetailPage,
};

export function registerAppRoutes() {
  group({ layout: Layout, auth: true }, () => {
    route("/", Home);
    route("/admin", Home);
    route("/sessions", SessionsPage);
    route("/admin/metrics", MetricsPage);
    route("/diagnostics", DiagnosticsPage);
    route("/settings", SettingsPage);

    for (const link of domainLinks) {
      route(link.href, domainPageBySegment[link.segment]);
      route(domainResourceRoutePath(link.segment), resourcePageBySegment[link.segment]);
    }
  });
}
