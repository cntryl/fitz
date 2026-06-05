import type { RouteAuthOptions } from "@askrjs/askr/router";
import { sessionService } from "./session-service";

export const sessionRouteAuth: RouteAuthOptions = {
  resolve: async ({ signal }) => {
    try {
      const session = await sessionService.getCurrentSession({ signal });

      return {
        session,
        user: session?.authenticated ? { username: session.username } : null,
      };
    } catch {
      return {
        session: null,
        user: null,
      };
    }
  },
  loginPath: "/login",
  guestRedirectTo: "/",
};
