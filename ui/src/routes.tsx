import { layout, route } from "@askrjs/askr/router";
import AppLayout from "./app";
import AdminHome from "./pages/admin-home";
import AdminLogin from "./pages/admin-login";

const app = layout(AppLayout);

route("/", () => app(<AdminHome />));
route("/login", () => app(<AdminLogin />));
route("/admin", () => app(<AdminHome />));
