import "../styles/index.css";
import { invalidateOnInterval } from "@askrjs/askr/data";
import { ThemeScope } from "@askrjs/themes/theme";
import { SYSTEM_OVERVIEW_KEY } from "@/features/system/system-query";
import { MESSAGING_TOPOLOGY_KEY } from "@/features/topology/topology-query";
import { appConfig } from "@/shared/config";

export default function RootLayout({ children }: { children?: unknown }) {
  invalidateOnInterval(MESSAGING_TOPOLOGY_KEY, {
    activeOn: "/admin/{family}",
    intervalMs: appConfig.dashboardPollIntervalMs,
    visibleOnly: true,
  });
  invalidateOnInterval(SYSTEM_OVERVIEW_KEY, {
    activeOn: "/admin/{family}",
    intervalMs: appConfig.dashboardPollIntervalMs,
    visibleOnly: true,
  });

  return (
    <ThemeScope defaultTheme="system" storageKey="fitz-admin-theme">
      {children}
    </ThemeScope>
  );
}
