import { client } from "./client";
import { createAdapter } from "./generated/api";

// Adapter boundary only: export the generated API instance. Services own app contracts.
export const apiv1 = createAdapter(client);

export { client };
export { createAdapter };
export type * from "./generated/types";
