import { createMutation } from "@askrjs/askr/data";
import type { LoginPayload } from "./session-models";
import { SESSION_QUERY_PREFIX } from "./session-query";
import { sessionService } from "./session-service";

export function createSignInMutation() {
  return createMutation<LoginPayload, void>({
    action: async (payload, { signal }) => {
      await sessionService.signIn(payload, { signal });
    },
    affects: () => [SESSION_QUERY_PREFIX],
    afterSuccess: "invalidate",
  });
}

export function createSignOutMutation() {
  return createMutation<undefined, void>({
    action: async (_input, { signal }) => {
      await sessionService.signOut({ signal });
    },
    affects: () => [SESSION_QUERY_PREFIX],
    afterSuccess: "invalidate",
  });
}
