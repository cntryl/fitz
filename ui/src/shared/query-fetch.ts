export interface QueryFetchContext {
  signal: AbortSignal;
}

export type QueryFetch<T> = (context: QueryFetchContext) => Promise<T>;

export function stableQueryFetch<T>(
  cache: Map<string, QueryFetch<T>>,
  key: string,
  createFetch: () => QueryFetch<T>,
) {
  const existing = cache.get(key);

  if (existing) {
    return existing;
  }

  const fetch = createFetch();
  cache.set(key, fetch);
  return fetch;
}
