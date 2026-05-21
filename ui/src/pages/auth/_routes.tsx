import { group, route } from "@askrjs/askr/router";
import Layout from "./_layout";
import Login from "./login";
import Logout from "./logout";

export function registerAuthRoutes() {
  group({ layout: Layout }, () => {
    route("/logout", Logout);
    route("/login", Login);
  });
}
