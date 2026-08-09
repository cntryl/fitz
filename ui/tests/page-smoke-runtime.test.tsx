import { describe, expect, it } from "vite-plus/test";
import { createQuery } from "@askrjs/askr/data";
import { createQueryTestRegistry, queryState } from "@askrjs/askr/testing";
import { mountRoute } from "./page-smoke/harness";

const ROUTED_QUERY_KEY = "test/page-smoke-runtime";
const fetchRoutedQuery = async () => ({ source: "network" });

function RoutedQueryFixture() {
  const query = createQuery({
    key: ROUTED_QUERY_KEY,
    fetch: fetchRoutedQuery,
  });

  return <span>{query.data?.source}</span>;
}

describe("page smoke runtime", () => {
  it("injects query fixtures into routed renders", async () => {
    const registry = createQueryTestRegistry();
    registry.set(ROUTED_QUERY_KEY, queryState.fresh({ source: "registry" }));

    const root = await mountRoute(
      "/runtime-fixture",
      "/runtime-fixture",
      RoutedQueryFixture,
      registry.runtime,
    );

    expect(root.textContent).toBe("registry");
  });
});
