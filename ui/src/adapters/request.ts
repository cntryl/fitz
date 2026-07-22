import type { ServiceRequestOptions } from "@/shared/errors/api";

export function apiParams<P>(params: P, options: ServiceRequestOptions = {}) {
  return { ...options, params };
}

export function apiQuery<Q>(query: Q, options: ServiceRequestOptions = {}) {
  return { ...options, query };
}

export function apiParamsQuery<P, Q>(params: P, query: Q, options: ServiceRequestOptions = {}) {
  return { ...options, params, query };
}

export function apiBody<B>(body: B, options: ServiceRequestOptions = {}) {
  return { ...options, body };
}
