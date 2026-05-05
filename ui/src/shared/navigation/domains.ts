export interface DomainLink {
  href: string;
  title: string;
  description: string;
}

export const domainLinks: DomainLink[] = [
  {
    href: "/queue",
    title: "Queue",
    description: "Queue stats, realms, and future dead-letter drill-downs.",
  },
  {
    href: "/kv",
    title: "KV",
    description: "Key-value broker statistics and realm browsing.",
  },
  {
    href: "/lease",
    title: "Lease",
    description: "Lease realm coverage and live lease load.",
  },
  {
    href: "/notice",
    title: "Notice",
    description: "Notice fanout statistics and realm inventory.",
  },
  {
    href: "/rpc",
    title: "RPC",
    description: "Pending RPC work and worker registrations.",
  },
  {
    href: "/schedule",
    title: "Schedule",
    description: "Scheduled execution health and realm coverage.",
  },
  {
    href: "/stream",
    title: "Stream",
    description: "Stream throughput, subscriptions, and realm structure.",
  },
];
