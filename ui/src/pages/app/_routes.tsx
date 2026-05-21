import { group, route } from "@askrjs/askr/router";
import Layout from "./_layout";
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
import StreamPage from "./stream";

export function registerAppRoutes() {
  group({ layout: Layout, auth: true }, () => {
    route("/", Home);
    route("/admin", Home);
    route("/sessions", SessionsPage);
    route("/metrics", MetricsPage);
    route("/queue", QueuePage);
    route("/queue/{realm}/{area}/{resource}", QueueResourcePage);
    route("/kv", KvPage);
    route("/kv/{realm}/{area}/{resource}", ResourceDetailPage);
    route("/lease", LeasePage);
    route("/lease/{realm}/{area}/{resource}", ResourceDetailPage);
    route("/notice", NoticePage);
    route("/notice/{realm}/{area}/{resource}", ResourceDetailPage);
    route("/rpc", RpcPage);
    route("/rpc/{realm}/{area}/{resource}", ResourceDetailPage);
    route("/schedule", SchedulePage);
    route("/schedule/{realm}/{area}/{resource}", ResourceDetailPage);
    route("/stream", StreamPage);
    route("/stream/{realm}/{area}/{resource}", ResourceDetailPage);
  });
}
