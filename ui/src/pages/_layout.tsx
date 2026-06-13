import "../styles/index.css";
import { invalidate } from "@askrjs/askr/data";
import { timer } from "@askrjs/askr/resources";
import { ThemeProvider } from "@askrjs/themes/theme";
import { MESSAGING_TOPOLOGY_KEY } from "@/features/topology/topology-query";
import { appConfig } from "@/shared/config";

export default function RootLayout({ children }: { children?: unknown }) {
  timer(appConfig.dashboardPollIntervalMs, () => {
    if (typeof document !== "undefined" && document.visibilityState === "hidden") {
      return;
    }

    const pathname = typeof window === "undefined" ? "" : window.location.pathname;

    if (pathname === "/" || pathname === "/admin") {
      invalidate(MESSAGING_TOPOLOGY_KEY);
    }
  });

  return (
    <ThemeProvider defaultTheme="system" storageKey="fitz-admin-theme">
      {children}
    </ThemeProvider>
  );
}
