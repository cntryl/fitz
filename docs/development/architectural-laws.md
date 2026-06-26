# The Seven Fitz Architectural Laws

These laws are hard constraints for Fitz design and review. They exist to keep Fitz a runtime of narrow, composable primitives rather than a blurred platform of convenience semantics.

If a proposed change violates one of these laws, the change is wrong unless the architecture itself is being deliberately redefined.

## Law 1: A Domain May Only Provide Its Own Guarantees

No Fitz domain may silently inherit, emulate, or imply another domain's guarantees.

Examples:

- Notice must not imply Stream durability.
- Stream must not imply Queue reservation semantics.
- Queue must not imply Lease ownership semantics.
- RPC must not imply Queue backlog durability.
- Schedule must not imply workflow execution semantics.
- KV must not imply historical replay.

A domain may compose with another domain explicitly.
It may not absorb another domain's contract.

If a feature makes one domain feel like another, the feature is wrong.

## Law 2: Ephemeral And Durable Behavior Must Never Be Ambiguous

Every Fitz operation, state transition, and API surface must be clearly classified as either:

- ephemeral
- durable

This classification must be obvious in both documentation and implementation.

Nothing may appear durable unless it survives restart.
Nothing may appear recoverable unless recovery is explicitly defined.

Examples:

- Notice delivery is ephemeral.
- RPC pending state is ephemeral.
- Queue backlog is durable according to the configured queue write policy.
- Queue inflight processing is ephemeral.
- Stream committed history is durable.
- KV committed state is durable.
- Lease ownership is ephemeral.
- Schedule intent is durable.

Ambiguity here is a correctness bug.

## Law 3: Disconnect Kills Session State

Session-scoped state dies when the session dies unless a domain explicitly defines durable recovery.

A reconnect is a new session.
It is not continuation by implication.

Fitz must not silently restore:

- Notice subscriptions
- RPC handlers
- Lease ownership
- Queue inflight work
- ephemeral waiters
- live cursors
- transient claims

Only explicitly durable state may survive restart or reconnect.

Examples of durable state:

- Stream committed records
- KV committed writes
- Queue enqueued backlog that reached durable storage under the configured queue write policy
- Schedule persisted intent

If a client must resume something, the client must do so explicitly against a domain that supports it.

## Law 4: Client-Visible Guarantees Must Match Persisted Reality

Fitz may never advertise a stronger guarantee than the storage and recovery model can actually uphold.

If something is only true in memory, it must be described as in-memory.
If something survives restart, it must be backed by actual durable state.

Fitz must never imply:

- replay where none exists
- exactly-once where only at-least-once exists
- ownership continuity where restart breaks it
- ordering guarantees that are not actually enforced
- durable recovery of transient session behavior

Truth in guarantees matters more than convenience.

If the implementation cannot prove a guarantee, Fitz must not claim it.

## Law 5: Domains Define Physics, Not Policy

Fitz domains provide primitive behavior.
They do not encode application policy, orchestration logic, or product behavior.

Examples of physics:

- Queue may redeliver work.
- Stream may replay committed history.
- Lease may fence ownership within broker lifetime.
- Schedule may persist future timing intent.

Examples of policy:

- retry 5 times then alert
- escalate after repeated failure
- run A then B then compensate with C
- preserve a consumer checkpoint forever
- route failures based on business category

Policy belongs outside the primitive.

If a change makes a domain decide user workflow instead of enforcing domain mechanics, the change is wrong.

## Law 6: Composition Must Be Explicit

If multiple domains are used together, the composition must be explicit in code, API shape, and guarantees.

Fitz must never hide cross-domain composition behind convenience semantics that blur ownership of responsibility.

Examples:

- Notice + Stream must remain explicit dual-write semantics if both are used.
- Schedule + Queue must remain explicit timing-to-work composition.
- Lease + KV must remain explicit coordination around state mutation.
- Queue + Stream must remain explicit if work events are also recorded as history.

No domain may masquerade as a fused version of two domains.

Composition is allowed.
Fusion is not.

## Law 7: Observability Must Never Define Behavior

Logs, metrics, traces, projections, dashboards, and telemetry are descriptive only.

They may observe:

- execution
- latency
- failure
- counts
- state transitions

They may not control:

- retries
- ownership
- recovery
- scheduling
- routing
- correctness decisions

Telemetry may be lossy.
Correctness may not.

If disabling observability changes Fitz behavior, the design is wrong.

## Interpretation Rule

When evaluating a proposed change, ask:

1. Does this add another domain's guarantee here?
2. Does this blur ephemeral versus durable behavior?
3. Does this preserve the rule that disconnect kills session state?
4. Does the client-visible contract match persisted reality?
5. Is this primitive physics or user policy?
6. Is cross-domain composition explicit?
7. Would Fitz still behave correctly if observability were removed?

If any answer is wrong, reject the change.

## Final Invariant

Fitz must remain a runtime of narrow, composable primitives:

- Notice = live awareness
- Stream = durable history
- KV = authoritative current state
- Queue = durable work distribution with configurable write-policy latency/durability tradeoffs
- RPC = live execution dispatch
- Lease = explicit ownership coordination
- Schedule = durable timing intent

The architecture stays clean only if these meanings stay stable.
