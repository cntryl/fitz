import type { ServiceRequestOptions } from "@/shared/errors/api";
import { apiRouteFamilySegment } from "./domains";

export interface RouteFamilyRequestOptions extends ServiceRequestOptions {
  routeFamily?: number | string;
}

export function routeFamilyRequest(options: RouteFamilyRequestOptions = {}) {
  const { routeFamily, ...requestOptions } = options;
  return {
    family: apiRouteFamilySegment(routeFamily),
    requestOptions,
  };
}
