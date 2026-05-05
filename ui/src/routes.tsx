import { group, route } from "@askrjs/askr/router";
import AppLayout from "@/app";
import AdminHome from "@/pages/admin-home";
import AdminLogin from "@/pages/admin-login";
import Home from "@/pages/home";
import QueuePage from "@/pages/queue";
import QueueResourcePage from "@/pages/queue-resource";
import KvPage from "@/pages/kv";
import LeasePage from "@/pages/lease";
import NoticePage from "@/pages/notice";
import RpcPage from "@/pages/rpc";
import SchedulePage from "@/pages/schedule";
import StreamPage from "@/pages/stream";

group({ layout: AppLayout }, () => {
  route("/", Home);
  route("/admin", AdminHome);
  route("/login", AdminLogin);
  route("/queue", QueuePage);
  route("/queue/{realm}/{area}/{resource}", QueueResourcePage);
  route("/kv", KvPage);
  route("/lease", LeasePage);
  route("/notice", NoticePage);
  route("/rpc", RpcPage);
  route("/schedule", SchedulePage);
  route("/stream", StreamPage);
});
