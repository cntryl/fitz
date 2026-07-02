import { Link } from "@askrjs/askr/router";
import { Button, Stack } from "@askrjs/themes/components";
import { createDomainSidebar } from "@/components/shared/domain-sidebar";
import type { QueueResourceOverview } from "@/features/queue/queue-resource-models";
import { domainHref } from "@/shared/navigation/domains";
import { humanizeSeconds } from "./queue-resource-presenters";

export function createQueueResourceSidebar({
  data,
  onRefresh,
  scopeLabel,
}: {
  data?: QueueResourceOverview | null;
  onRefresh: () => unknown;
  scopeLabel: string;
}) {
  return createDomainSidebar({
    data,
    title: "Scope summary",
    description: scopeLabel,
    stats: (current) => [
      { label: "Realm", value: current.detail.realm },
      { label: "Area", value: current.detail.area },
      { label: "Resource", value: current.detail.resource },
      { label: "Ready", value: current.detail.messagesReady },
      { label: "Inflight", value: current.detail.messagesInflight },
      { label: "Dead-lettered", value: current.detail.messagesDeadLettered },
      { label: "Delayed", value: current.detail.messagesDelayed },
      {
        label: "Oldest age",
        value: humanizeSeconds(current.detail.oldestMessageAgeSeconds),
        note: "Point-in-time snapshot",
      },
    ],
    footer: (
      <Stack gap="3">
        <Link href={domainHref("queue")}>Back to Queue</Link>
        <Button onPress={onRefresh}>Refresh</Button>
      </Stack>
    ),
  });
}
