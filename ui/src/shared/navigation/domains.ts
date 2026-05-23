import {
  DatabaseIcon,
  BoxesIcon,
  LayoutDashboardIcon,
  LineChartIcon,
  MessagesSquareIcon,
  NetworkIcon,
  RadioTowerIcon,
  Rows3Icon,
  TimerResetIcon,
  KeyRoundIcon,
  UsersIcon,
} from "@askrjs/lucide";

export interface DomainLink {
  href: string;
  title: string;
  description: string;
  icon: typeof BoxesIcon;
}

export interface ShellLink {
  href: string;
  title: string;
  icon: typeof BoxesIcon;
}

export const shellLinks: ShellLink[] = [
  {
    href: "/",
    title: "Dashboard",
    icon: LayoutDashboardIcon,
  },
  {
    href: "/sessions",
    title: "Sessions",
    icon: UsersIcon,
  },
  {
    href: "/metrics",
    title: "Metrics",
    icon: LineChartIcon,
  },
];

export const domainLinks: DomainLink[] = [
  {
    href: "/lease",
    title: "Lease",
    description: "Lease realm coverage and live lease load.",
    icon: KeyRoundIcon,
  },
  {
    href: "/notice",
    title: "Notice",
    description: "Notice fanout statistics and realm inventory.",
    icon: MessagesSquareIcon,
  },
  {
    href: "/rpc",
    title: "RPC",
    description: "Pending RPC work and worker registrations.",
    icon: NetworkIcon,
  },
  {
    href: "/schedule",
    title: "Schedule",
    description: "Scheduled execution health and realm coverage.",
    icon: TimerResetIcon,
  },
  {
    href: "/queue",
    title: "Queue",
    description: "Queue stats, realms, and future dead-letter drill-downs.",
    icon: Rows3Icon,
  },
  {
    href: "/stream",
    title: "Stream",
    description: "Stream throughput, subscriptions, and realm structure.",
    icon: RadioTowerIcon,
  },
  {
    href: "/kv",
    title: "KV",
    description: "Key-value broker statistics and realm browsing.",
    icon: DatabaseIcon,
  },
];
