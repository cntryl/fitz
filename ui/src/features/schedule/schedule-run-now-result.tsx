import { Alert } from "@askrjs/themes/components";
import type { ScheduleRunNowResponse } from "@/adapters";

export default function ScheduleRunNowResult(props: { result: ScheduleRunNowResponse | null }) {
  if (!props.result) return null;
  const accepted = props.result.accepted_handoffs > 0;
  return (
    <Alert
      variant={accepted ? "success" : "warning"}
      title={accepted ? "Schedule handoff accepted" : "No handoff accepted"}
      description={`${props.result.route}: ${props.result.matched_subscriptions} matched, ${props.result.attempted_handoffs} attempted, ${props.result.accepted_handoffs} accepted (${props.result.outcome}).`}
    />
  );
}
