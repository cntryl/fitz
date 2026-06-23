import { group, route, type RouteHandler } from "@askrjs/askr/router";
import Layout from "./_layout";
import DiagnosticsPage from "./diagnostics";
import Home from "./home";
import KvPage from "./kv";
import LeasePage from "./lease";
import MetricsPage from "./metrics";
import NoticePage from "./notice";
import QueuePage from "./queue";
import QueueResourcePage from "./queue-resource";
import ResourceDetailPage from "./resource-detail";
import RpcPage from "./rpc";
import SchedulePage from "./schedule";
import SessionsPage from "./sessions";
import SettingsPage from "./settings";
import StreamPage from "./stream";
import {
  domainLinks,
  domainResourceRoutePath,
  type DomainSegment,
} from "@/shared/navigation/domains";

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
