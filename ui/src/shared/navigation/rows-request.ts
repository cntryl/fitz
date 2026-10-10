/**
 * Detail pages load data rows (KV rows, stream records, queue messages) only when
 * the operator asks. The request lives in the URL so paging, reloads, and shared
 * links keep it.
 */
export const ROWS_REQUEST_PARAM = "rows";

interface QueryReader {
  get(name: string): string | null;
}

/** True when the URL asks for rows, directly or through a row-paging parameter. */
export function hasRowsRequest(query: QueryReader, pagingParams: readonly string[] = []) {
  return (
    query.get(ROWS_REQUEST_PARAM) === "1" || pagingParams.some((name) => query.get(name) !== null)
  );
}

/** Starts a row query string that carries the explicit request. */
export function rowsRequestQuery() {
  return new URLSearchParams({ [ROWS_REQUEST_PARAM]: "1" });
}

interface QuerySnapshot {
  toJSON(): Record<string, string | string[]>;
}

/**
 * Builds a link that reveals one more section while keeping every param already on
 * the URL, so asking for transitions or transactions does not unload loaded rows.
 */
export function revealSectionHref(path: string, current: QuerySnapshot, param: string) {
  const query = new URLSearchParams();

  for (const [name, value] of Object.entries(current.toJSON())) {
    for (const item of Array.isArray(value) ? value : [value]) query.append(name, item);
  }
  query.set(param, "1");

  return `${path}?${query.toString()}`;
}
