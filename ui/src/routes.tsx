import { group, route } from "@askrjs/askr/router";
import AppLayout from "./app";
import AdminHome from "./pages/admin-home";
import AdminLogin from "./pages/admin-login";

group({ layout: AppLayout }, () => {
  route("/", AdminHome);
  route("/login", AdminLogin);
  route("/admin", AdminHome);
});
