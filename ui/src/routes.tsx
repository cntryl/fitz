import { group, registerRoutes, route } from "@askrjs/askr/router";
import AppLayout from "@/app";
import GuestLayout from "@/components/shared/guest-layout";
import AdminHome from "@/pages/admin-home";
import AdminLogin from "@/pages/admin-login";
import Home from "@/pages/home";
import SessionsPage from "@/pages/sessions";
import QueuePage from "@/pages/queue";
import QueueResourcePage from "@/pages/queue-resource";
import KvPage from "@/pages/kv";
import LeasePage from "@/pages/lease";
import NoticePage from "@/pages/notice";
import RpcPage from "@/pages/rpc";
import SchedulePage from "@/pages/schedule";
import StreamPage from "@/pages/stream";
import { sessionRouteAuth } from "@/features/session/session-auth";

registerRoutes(
  () => {
    group({ layout: GuestLayout }, () => {
      route("/", Home);

      group({ auth: "guest" }, () => {
        route("/login", AdminLogin);
      });
    });

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
  },
  { auth: sessionRouteAuth },
);
