import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { createRouteRegistry, currentRoute, navigate, route } from "@askrjs/askr/router";
import { renderRoute, type RenderResult } from "@askrjs/askr/testing";
import type { createQueueResourceQuery as CreateQueueResourceQuery } from "@/features/queue/queue-resource-query";
import { serviceContractMocks } from "./service-contract/setup";

const mocks = serviceContractMocks();
let mounted: RenderResult | null = null;
let renderedQuery: ReturnType<typeof CreateQueueResourceQuery> | null = null;
let queryFactory: typeof CreateQueueResourceQuery;

afterEach(() => {
  mounted?.cleanup();
  mounted = null;
  renderedQuery = null;
  window.history.pushState({}, "", "/");
});

function QueueFamilyProbe() {
  const activeRoute = currentRoute();
  const { area, realm, resource } = activeRoute.params;
  const query = queryFactory({ area, realm, resource });
  renderedQuery = query;
  return <p data-family={activeRoute.params.family}>{query.data?.resource ?? "loading"}</p>;
}

describe("routed family query binding", () => {
  it.each(["primary", "secondary"])(
    "requests the destination family for resource %s while navigation stages before history commit",
    async (destinationResource) => {
      // Arrange
      queryFactory = (await import("@/features/queue/queue-resource-query"))
        .createQueueResourceQuery;
      const registry = createRouteRegistry(() => {
        route("/admin/{family}/queue/{realm}/{area}/{resource}", QueueFamilyProbe);
      });
      mounted = await renderRoute({
        registry,
        url: "/admin/7/queue/default/ops/primary",
      });
      await renderedQuery?.refresh();
      expect(mocks.apiv1.getQueueResource).toHaveBeenCalled();
      mocks.apiv1.getQueueResource.mockClear();

      // Act
      navigate(`/admin/8/queue/default/ops/${destinationResource}`, { scroll: "preserve" });
      await vi.waitFor(() =>
        expect(window.location.pathname).toBe(`/admin/8/queue/default/ops/${destinationResource}`),
      );
      await renderedQuery?.refresh();

      // Assert
      expect(mounted.root.querySelector("[data-family='8']")).not.toBeNull();
      expect(mocks.apiv1.getQueueResource.mock.calls[0]?.[0]).toEqual(
        expect.objectContaining({
          params: { area: "ops", family: "8", realm: "default", resource: destinationResource },
        }),
      );
    },
  );
});
