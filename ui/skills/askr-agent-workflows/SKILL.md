---
name: askr-agent-workflows
description: Use when building Askr UI for AI or agent workflows, including prompts, runs, streaming output, tool timelines, approvals, cancellation, retries, audit logs, persistence, and event-sourced run state.
---

# Askr Agent Workflows

Use this for user-facing agentic product flows.

## Inspect First

- Existing agent API adapters and run/event schemas.
- Feature folders for prompts, runs, tools, approvals, or artifacts.
- Realtime/event-streaming utilities.
- Product requirements for human approval, audit, and cancellation.

## Run State Model

Model agent work as a run, not a one-off response:

- `draft`: user is composing input.
- `queued`: backend accepted the run but no work is visible.
- `running`: events or tokens are arriving.
- `requires-action`: user approval, credential, or clarification is needed.
- `cancelling`: cancel requested, waiting for acknowledgement.
- `succeeded`, `failed`, `cancelled`: terminal states.

## Event-Sourced UX

- Treat the server event log as the source of truth.
- Apply events idempotently by event ID or sequence.
- Show optimistic local intent separately from confirmed server state.
- Preserve `lastEventId` or cursor for resume/reconnect.
- On reconnect, request events after the last seen cursor and refetch the affected projection if a gap is detected.
- When projections lag, display "saving", "syncing", or "finalizing" rather than pretending completion.

## UI Composition

- Prompt composer owns draft state and submit intent.
- Run timeline renders messages, tool calls, approvals, errors, and artifacts.
- Approval cards make the requested action, risk, and resulting command explicit.
- Cancel and retry controls reflect backend acknowledgement, not just button clicks.

## Avoid

- A single `loading` boolean for the whole run.
- Replacing the timeline wholesale when streaming events arrive.
- Hiding tool calls or approvals in unstructured text.
- Retrying runs without idempotency or parent-run linkage.
- Losing partial output on refresh or reconnect.

## Checks

- Run states map to visible UI.
- Event application is idempotent and ordered.
- Cancellation, retry, approval, and failure paths are covered.
- The user can understand what the agent did and what still needs attention.
