import type { RouteAuthOptions } from "@askrjs/askr/router";
import { sessionService } from "./session-service";

export const sessionRouteAuth: RouteAuthOptions = {
  resolve: async ({ signal }) => {
    try {
      const session = await sessionService.getCurrentSession({ signal });

      return {
        authenticated: Boolean(session?.authenticated),
        principal: session?.authenticated
          ? {
              id: session.username || "open-access",
              subject: session.username || "open-access",
              username: session.username || undefined,
            }
          : null,
        session: null,
        tenant: null,
      };
    } catch {
      return {
        authenticated: false,
        principal: null,
        session: null,
        tenant: null,
      };
    }
  },
  loginPath: "/login",
};
