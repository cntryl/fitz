import { layout, route } from "@askrjs/askr/router";
import AppLayout from "./app";
import Home from "./pages/home";
import About from "./pages/about";
import Example from "./pages/example";

// Register routes at module-load time
const app = layout(AppLayout);

route("/", () => app(<Home />));
route("/about", () => app(<About />));
route("/example", () => app(<Example />));
