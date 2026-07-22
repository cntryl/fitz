import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { cleanupApp, createSPA } from "@askrjs/askr/boot";
import { createInvalidationRecorder } from "@askrjs/askr/testing";
import { SYSTEM_OVERVIEW_KEY } from "@/features/system/system-query";
import { MESSAGING_TOPOLOGY_KEY } from "@/features/topology/topology-query";
import RootLayout from "@/pages/_layout";
import { appConfig } from "@/shared/config";

function setDocumentVisibility(visibilityState: DocumentVisibilityState) {
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    get: () => visibilityState,
  });
}

function LayoutHarness() {
  return (
    <RootLayout>
      <main>Layout polling harness</main>
    </RootLayout>
  );
}

async function mountLayout(path: string, routePath = path) {
  cleanupApp("app");
  document.body.innerHTML = '<div id="app"></div>';
  window.history.pushState({}, "", path);

  const root = document.getElementById("app");
  if (!root) {
    throw new Error("Missing test app root");
  }

  await createSPA({
    root,
    routes: [{ handler: LayoutHarness, path: routePath }],
  });

  await new Promise<void>((resolve) => queueMicrotask(() => resolve()));
  return root;
}

afterEach(() => {
  cleanupApp("app");
  document.body.innerHTML = "";
  setDocumentVisibility("visible");
  vi.useRealTimers();
});

describe("root layout dashboard polling", () => {
  it("invalidates overview sources on the family dashboard route interval", async () => {
    vi.useFakeTimers();
    const recorder = createInvalidationRecorder();

    try {
      await mountLayout("/admin/1", "/admin/{family}");
      vi.advanceTimersByTime(appConfig.dashboardPollIntervalMs);

      expect(recorder.calls).toEqual([
        {
          markPendingWrite: false,
          prefix: MESSAGING_TOPOLOGY_KEY,
        },
        {
          markPendingWrite: false,
          prefix: SYSTEM_OVERVIEW_KEY,
        },
      ]);
    } finally {
      recorder.stop();
    }
  });

  it("does not invalidate topology outside dashboard routes", async () => {
    vi.useFakeTimers();
    const recorder = createInvalidationRecorder();

    try {
      await mountLayout("/admin/1/metrics", "/admin/{family}/metrics");
      vi.advanceTimersByTime(appConfig.dashboardPollIntervalMs);

      expect(recorder.calls).toEqual([]);
    } finally {
      recorder.stop();
    }
  });

  it("does not invalidate overview sources while the document is hidden", async () => {
    vi.useFakeTimers();
    setDocumentVisibility("hidden");
    const recorder = createInvalidationRecorder();

    try {
      await mountLayout("/admin/1", "/admin/{family}");
      vi.advanceTimersByTime(appConfig.dashboardPollIntervalMs);

      expect(recorder.calls).toEqual([]);
    } finally {
      recorder.stop();
    }
  });
});
