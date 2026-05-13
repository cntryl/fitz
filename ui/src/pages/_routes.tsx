import { registerRoutes } from "@askrjs/askr/router";
import { sessionRouteAuth } from "@/features/session/session-auth";
import { registerAppRoutes } from "./app/_routes";
import { registerPublicRoutes } from "./public/_routes";

registerRoutes(
  () => {
    registerPublicRoutes();
    registerAppRoutes();
  },
  { auth: sessionRouteAuth },
);
