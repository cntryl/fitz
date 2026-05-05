import { createQuery } from "@askrjs/askr/data";
import { sessionService } from "./session-service";

const CURRENT_SESSION_KEY = "session:current";

export function createCurrentSessionQuery() {
  return createQuery({
    key: CURRENT_SESSION_KEY,
    fetch: ({ signal }) => sessionService.getCurrentSession({ signal }),
  });
}
