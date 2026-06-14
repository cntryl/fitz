import "../styles/index.css";
import { invalidateOnInterval } from "@askrjs/askr/data";
import { ThemeProvider } from "@askrjs/themes/theme";
import { MESSAGING_TOPOLOGY_KEY } from "@/features/topology/topology-query";
import { appConfig } from "@/shared/config";

export default function RootLayout({ children }: { children?: unknown }) {
  invalidateOnInterval(MESSAGING_TOPOLOGY_KEY, {
    activeOn: ["/", "/admin"],
    intervalMs: appConfig.dashboardPollIntervalMs,
    visibleOnly: true,
  });

  return (
    <ThemeProvider defaultTheme="system" storageKey="fitz-admin-theme">
      {children}
    </ThemeProvider>
  );
}
