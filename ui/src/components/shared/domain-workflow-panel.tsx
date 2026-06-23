import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { CheckCircle2Icon, HelpCircleIcon, WrenchIcon } from "@askrjs/lucide";
import { Flex, Stack } from "@askrjs/themes/layouts";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";

export interface DomainWorkflowPanelProps {
  archetype: string;
  diagnostics: string[];
  questions: string[];
  workflows: string[];
}

function WorkflowList({
  icon,
  items,
  title,
}: {
  icon: unknown;
  items: string[];
  title: string;
}) {
  return (
    <Stack gap="2" class="workflow-list">
      <Flex align="center" gap="2">
        {icon}
        <h3>{title}</h3>
      </Flex>
      <ul>
        <For each={items} by={(item) => item}>
          {(item) => <li>{item}</li>}
        </For>
      </ul>
    </Stack>
  );
}

export default function DomainWorkflowPanel({
  archetype,
  diagnostics,
  questions,
  workflows,
}: DomainWorkflowPanelProps) {
  return (
    <Card padding="sm" variant="default">
      <CardHeader>
        <CardTitle>{archetype}</CardTitle>
        <CardDescription>Operator workflow, health questions, and diagnostics boundaries.</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="workflow-panel-grid">
          <WorkflowList
            title="Workflows"
            icon={<CheckCircle2Icon aria-hidden="true" size={16} />}
            items={workflows}
          />
          <WorkflowList
            title="Questions"
            icon={<HelpCircleIcon aria-hidden="true" size={16} />}
            items={questions}
          />
          <WorkflowList
            title="Diagnostics"
            icon={<WrenchIcon aria-hidden="true" size={16} />}
            items={diagnostics}
          />
        </div>
        <Link class="text-link" href="/diagnostics">
          Open diagnostics
        </Link>
      </CardContent>
    </Card>
  );
}
