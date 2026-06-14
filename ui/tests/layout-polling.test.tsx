import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { cleanupApp, createSPA } from "@askrjs/askr/boot";
import { createInvalidationRecorder } from "@askrjs/askr/testing";
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

async function mountLayout(path: string) {
  cleanupApp("app");
  document.body.innerHTML = '<div id="app"></div>';
  window.history.pushState({}, "", path);

  const root = document.getElementById("app");
  if (!root) {
    throw new Error("Missing test app root");
  }

  await createSPA({
    root,
    routes: [{ handler: LayoutHarness, path }],
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
  it("invalidates topology on the dashboard route interval", async () => {
    vi.useFakeTimers();
    const recorder = createInvalidationRecorder();

    try {
      await mountLayout("/");
      vi.advanceTimersByTime(appConfig.dashboardPollIntervalMs);

      expect(recorder.calls).toEqual([
        {
          markPendingWrite: false,
          prefix: MESSAGING_TOPOLOGY_KEY,
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
      await mountLayout("/metrics");
      vi.advanceTimersByTime(appConfig.dashboardPollIntervalMs);

      expect(recorder.calls).toEqual([]);
    } finally {
      recorder.stop();
    }
  });

  it("does not invalidate topology while the document is hidden", async () => {
    vi.useFakeTimers();
    setDocumentVisibility("hidden");
    const recorder = createInvalidationRecorder();

    try {
      await mountLayout("/");
      vi.advanceTimersByTime(appConfig.dashboardPollIntervalMs);

      expect(recorder.calls).toEqual([]);
    } finally {
      recorder.stop();
    }
  });
});
