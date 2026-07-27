import { createSPA } from "@askrjs/askr/boot";
import { sessionRouteAuth } from "@/features/session/session-auth";
import { routeRegistry } from "./pages/_routes";

// Create and start the SPA
void createSPA({
  root: document.getElementById("app")!,
  registry: routeRegistry,
  auth: sessionRouteAuth,
});
