import { group, registerRoutes } from "@askrjs/askr/router";
import { sessionRouteAuth } from "@/features/session/session-auth";
import RootLayout from "./_layout";
import { registerAppRoutes } from "./app/_routes";
import { registerAuthRoutes } from "./auth/_routes";

registerRoutes(
  () => {
    group({ layout: RootLayout }, () => {
      registerAuthRoutes();
      registerAppRoutes();
    });
  },
  { auth: sessionRouteAuth },
);
