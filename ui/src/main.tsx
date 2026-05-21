import { createSPA } from "@askrjs/askr/boot";
import { getManifest } from "@askrjs/askr/router";
import { sessionRouteAuth } from "@/features/session/session-auth";

// Import routes (they auto-register)
import "./pages/_routes";

// Create and start the SPA
void createSPA({
  root: document.getElementById("app")!,
  manifest: getManifest(),
  auth: sessionRouteAuth,
});
