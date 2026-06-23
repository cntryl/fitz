import { For } from "@askrjs/askr/control";
import { Link } from "@askrjs/askr/router";
import { CheckCircle2Icon, HelpCircleIcon, WrenchIcon } from "@askrjs/lucide";

export interface DomainWorkflowPanelProps {
  archetype: string;
  diagnostics: string[];
  questions: string[];
  workflows: string[];
}

function WorkflowList({ icon, items, title }: { icon: unknown; items: string[]; title: string }) {
  return (
    <section class="workflow-list">
      <div class="workflow-list-heading">
        {icon}
        <h3>{title}</h3>
      </div>
      <ul>
        <For each={items} by={(item) => item}>
          {(item) => <li>{item}</li>}
        </For>
      </ul>
    </section>
  );
}

export default function DomainWorkflowPanel({
  archetype,
  diagnostics,
  questions,
  workflows,
}: DomainWorkflowPanelProps) {
  return (
    <section class="domain-workflow-panel" aria-label={`${archetype} operator guide`}>
      <div class="domain-workflow-header">
        <div>
          <p class="domain-header-kicker">Operator guide</p>
          <h2>{archetype}</h2>
          <p>Operator paths, health questions, and escalation boundaries.</p>
        </div>
        <Link class="text-link" href="/diagnostics">
          Diagnostics
        </Link>
      </div>
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
    </section>
  );
}
