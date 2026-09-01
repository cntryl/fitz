import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogOverlay,
  AlertDialogPortal,
  AlertDialogTitle,
} from "@askrjs/ui";
import { Alert, Button, Block } from "@askrjs/themes/components";
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
    <AlertDialog open={confirmationMessage != null} onOpenChange={onOpenChange}>
      <AlertDialogPortal>
        <AlertDialogOverlay />
        {confirmationMessage && copy ? (
          <AlertDialogContent role="alertdialog">
            <AlertDialogTitle>{copy.title}</AlertDialogTitle>

            <AlertDialogDescription>{copy.description}</AlertDialogDescription>

            {actionError ? (
              <Alert
                variant="danger"
                title={`${confirmationKind === "replay" ? "Replay" : "Purge"} failed`}
                description={formatUnknownError(actionError)}
              />
            ) : null}

            <Block direction="row" gap="xs" justify="end" wrap={true}>
              <AlertDialogCancel asChild>
                <Button variant="secondary" type="button" disabled={actionPending}>
                  Cancel
                </Button>
              </AlertDialogCancel>

              <Button
                variant={confirmationKind === "purge" ? "destructive" : undefined}
                type="button"
                onPress={() => onRunAction(confirmationKind!, confirmationMessage)}
                disabled={actionPending}
                aria-busy={actionPending}
              >
                {actionPending ? copy.pendingLabel : copy.confirmLabel}
              </Button>
            </Block>
          </AlertDialogContent>
        ) : null}
      </AlertDialogPortal>
    </AlertDialog>
  );
}
