import { createMutation, type Mutation } from "@/shared/query/mutation";
import type { LoginPayload } from "./session-models";
import { sessionService } from "./session-service";

const SESSION_QUERY_PREFIX = "session:";

export type SignInMutation = Mutation<LoginPayload, void>;
export type SignOutMutation = Mutation<undefined, void>;

export function createSignInMutation(): SignInMutation {
  return createMutation({
    action: async (payload, { signal }) => {
      await sessionService.signIn(payload, { signal });
    },
    affects: () => [SESSION_QUERY_PREFIX],
    afterSuccess: "invalidate",
  });
}

export function createSignOutMutation(): SignOutMutation {
  return createMutation({
    action: async (_input, { signal }) => {
      await sessionService.signOut({ signal });
    },
    affects: () => [SESSION_QUERY_PREFIX],
    afterSuccess: "invalidate",
  });
}
