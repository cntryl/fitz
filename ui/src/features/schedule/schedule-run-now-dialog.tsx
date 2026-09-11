import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogOverlay,
  AlertDialogPortal,
  AlertDialogTitle,
} from "@askrjs/ui";
import { Alert, Block, Button } from "@askrjs/themes/components";
import { formatUnknownError } from "@/shared/errors/format";

export interface ScheduleRunNowDialogProps {
  actionError?: unknown;
  actionPending: boolean;
  onOpenChange: (open: boolean) => void;
  onRunAction: () => void;
  open: boolean;
  routeLabel: string;
}

export default function ScheduleRunNowDialog(props: ScheduleRunNowDialogProps) {
  return (
    <AlertDialog open={props.open} onOpenChange={props.onOpenChange}>
      <AlertDialogPortal>
        <AlertDialogOverlay />
        <AlertDialogContent role="alertdialog">
          <AlertDialogTitle>Run schedule now?</AlertDialogTitle>
          <AlertDialogDescription>
            Send the stored payload for {props.routeLabel} only to live matching subscriptions. This
            is best-effort: cron and the next run remain unchanged, another scheduled occurrence may
            happen shortly afterward, and broker handoff acceptance is not downstream job
            completion.
          </AlertDialogDescription>
          {props.actionError ? (
            <Alert
              variant="danger"
              title="Run now failed"
              description={formatUnknownError(props.actionError)}
            />
          ) : null}
          <Block direction="row" gap="xs" justify="end" wrap={true}>
            <AlertDialogCancel asChild>
              <Button variant="secondary" type="button" disabled={props.actionPending}>
                Cancel
              </Button>
            </AlertDialogCancel>
            <Button
              type="button"
              onPress={props.onRunAction}
              disabled={props.actionPending}
              aria-busy={props.actionPending}
            >
              {props.actionPending ? "Running…" : "Run now"}
            </Button>
          </Block>
        </AlertDialogContent>
      </AlertDialogPortal>
    </AlertDialog>
  );
}
