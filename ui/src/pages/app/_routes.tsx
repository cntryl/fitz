import { group, route } from "@askrjs/askr/router";
import AppLayout from "./_layout";
import AdminHome from "./admin-home";
import KvPage from "./kv";
import LeasePage from "./lease";
import NoticePage from "./notice";
import QueuePage from "./queue";
import QueueResourcePage from "./queue-resource";
import RpcPage from "./rpc";
import SchedulePage from "./schedule";
import SessionsPage from "./sessions";
import StreamPage from "./stream";

export function registerAppRoutes() {
  group({ layout: AppLayout, auth: true }, () => {
    route("/admin", AdminHome);
    route("/sessions", SessionsPage);
    route("/queue", QueuePage);
    route("/queue/{realm}/{area}/{resource}", QueueResourcePage);
    route("/kv", KvPage);
    route("/lease", LeasePage);
    route("/notice", NoticePage);
    route("/rpc", RpcPage);
    route("/schedule", SchedulePage);
    route("/stream", StreamPage);
  });
}
