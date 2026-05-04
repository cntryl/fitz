import { FetchClient, addProductionStack } from "@fgrzl/fetch";

import { createAdapter as createapiv1Adapter } from "./apiv1.g";

export const client = addProductionStack(
  new FetchClient({
    credentials: "same-origin",
  }),
  {
    retry: {
      maxRetries: 2,
      delay: 1000,
    },
    rateLimit: {
      maxRequests: 100,
      windowMs: 60 * 1000,
    },
  },
);

export const apiv1 = createapiv1Adapter(client);

export { createAdapter } from "./apiv1.g";
export * from "./apiv1.g";
