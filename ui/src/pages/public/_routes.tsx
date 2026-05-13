import { group, route } from "@askrjs/askr/router";
import PublicLayout from "./_layout";
import AdminLogin from "./admin-login";
import Home from "./home";

export function registerPublicRoutes() {
  group({ layout: PublicLayout }, () => {
    route("/", Home);

    group({ auth: "guest" }, () => {
      route("/login", AdminLogin);
    });
  });
}
