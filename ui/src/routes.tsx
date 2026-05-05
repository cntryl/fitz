import { group, route } from "@askrjs/askr/router";
import AppLayout from "@/app";
import AdminHome from "@/pages/admin-home.page";
import AdminLogin from "@/pages/admin-login.page";

group({ layout: AppLayout }, () => {
  route("/", AdminHome);
  route("/login", AdminLogin);
  route("/admin", AdminHome);
});
