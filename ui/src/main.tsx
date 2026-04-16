import { createSPA } from "@askrjs/askr";
import { getRoutes } from "@askrjs/askr/router";

// Import routes (they auto-register)
import "./routes";

// Create and start the SPA
void createSPA({
  root: document.getElementById("app")!,
  routes: getRoutes(),
});
