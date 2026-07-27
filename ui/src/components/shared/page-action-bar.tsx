import { Link } from "@askrjs/askr/router";
import { For, Show } from "@askrjs/askr/control";
import { Button } from "@askrjs/themes/components";

export interface PageActionItem {
  href?: string;
  label: string;
  onPress?: () => void;
  variant?: "primary" | "outline" | "ghost";
}

export interface PageActionBarProps {
  actions: readonly PageActionItem[];
  description?: string;
  label?: string;
}

export default function PageActionBar({
  actions,
  description,
  label = "Page actions",
}: PageActionBarProps) {
  const actionItems = [...actions];

  if (actions.length === 0 && !description) {
    return null;
  }

  return (
    <section class="page-action-bar" aria-label={label}>
      {description ? <p>{description}</p> : null}
      <Show when={actions.length > 0}>
        <div class="page-action-bar-actions">
          <For each={actionItems} by={(action) => `${action.label}:${action.href ?? "button"}`}>
            {(action) =>
              action.href ? (
                <Link class="page-action-link" href={action.href}>
                  {action.label}
                </Link>
              ) : (
                <Button
                  type="button"
                  variant={action.variant ?? "outline"}
                  onPress={action.onPress ?? (() => undefined)}
                >
                  {action.label}
                </Button>
              )
            }
          </For>
        </div>
      </Show>
    </section>
  );
}
