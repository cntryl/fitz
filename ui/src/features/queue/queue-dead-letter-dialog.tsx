import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogOverlay,
  DialogPortal,
  DialogTitle,
} from "@askrjs/ui";
import { Alert, Button, Inline } from "@askrjs/themes/components";
import type { DeadLetterMessage } from "@/features/queue/queue-models";
import { formatUnknownError } from "@/shared/errors/format";

export interface QueueDeadLetterDialogProps {
  actionError?: unknown;
  actionPending: boolean;
  confirmationKind: "replay" | "purge" | null;
  confirmationMessage: DeadLetterMessage | null;
  onOpenChange: (open: boolean) => void;
  onRunAction: (kind: "replay" | "purge", message: DeadLetterMessage) => void;
  scopeLabel: string;
}

export function deadLetterDialogCopy(
  kind: "replay" | "purge" | null,
  messageId: number,
  scopeLabel: string,
) {
  return kind === "replay"
    ? {
        confirmLabel: "Replay message",
        pendingLabel: "Replaying...",
        title: "Replay dead-letter message?",
        description: `Replay message ${messageId} in ${scopeLabel}.`,
      }
    : {
        confirmLabel: "Purge message",
        pendingLabel: "Purging...",
        title: "Purge dead-letter message?",
        description: `Purge message ${messageId} from ${scopeLabel}. This is permanent.`,
      };
}

export default function QueueDeadLetterDialog({
  actionError,
  actionPending,
  confirmationKind,
  confirmationMessage,
  onOpenChange,
  onRunAction,
  scopeLabel,
}: QueueDeadLetterDialogProps) {
  const copy = confirmationMessage
    ? deadLetterDialogCopy(confirmationKind, confirmationMessage.messageId, scopeLabel)
    : null;

  return (
    <Dialog open={confirmationMessage != null} modal onOpenChange={onOpenChange}>
      <DialogPortal>
        <DialogOverlay class="dialog-overlay" />
        {confirmationMessage && copy ? (
          <DialogContent class="dialog-content" role="alertdialog">
            <DialogTitle>{copy.title}</DialogTitle>

            <DialogDescription>{copy.description}</DialogDescription>

            {actionError ? (
              <Alert
                variant="danger"
                title={`${confirmationKind === "replay" ? "Replay" : "Purge"} failed`}
                description={formatUnknownError(actionError)}
              />
            ) : null}

            <Inline gap="2" justify="end" wrap="wrap">
              <DialogClose asChild>
                <Button variant="secondary" type="button" disabled={actionPending}>
                  Cancel
                </Button>
              </DialogClose>

              <Button
                variant={confirmationKind === "purge" ? "destructive" : undefined}
                type="button"
                onPress={() => onRunAction(confirmationKind!, confirmationMessage)}
                disabled={actionPending}
                aria-busy={actionPending}
              >
                {actionPending ? copy.pendingLabel : copy.confirmLabel}
              </Button>
            </Inline>
          </DialogContent>
        ) : null}
      </DialogPortal>
    </Dialog>
  );
}
