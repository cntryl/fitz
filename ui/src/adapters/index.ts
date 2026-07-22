import { clientOptions } from "./client";
import { createApiClient } from "./generated/api";

// Adapter boundary only: export the generated API instance. Services own app contracts.
export const apiv1 = createApiClient(clientOptions);

export { clientOptions };
export { api, createApiClient } from "./generated/api";
export { apiBody, apiParams, apiParamsQuery, apiQuery } from "./request";
export type * from "./generated";
