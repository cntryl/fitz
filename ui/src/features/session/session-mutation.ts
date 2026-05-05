import { createMutation } from "@askrjs/askr/data";
import type { LoginPayload } from "./session-models";
import { sessionService } from "./session-service";

const SESSION_QUERY_PREFIX = "session:";

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
