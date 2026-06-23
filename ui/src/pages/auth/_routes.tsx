import { group, lazy, route } from "@askrjs/askr/router";
import Layout from "./_layout";

const Login = lazy(() => import("./login"));
const Logout = lazy(() => import("./logout"));

export function registerAuthRoutes() {
  group({ layout: Layout }, () => {
    route("/logout", Logout);
    route("/login", Login);
  });
}
