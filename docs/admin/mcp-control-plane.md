# MCP Control-Plane Safety

MCP is an AI-facing control-plane interface over the same operational read models used by REST and the admin UI. It is not a Fitz domain, not a privileged backdoor, and not a separate authorization system.

## Safety Model

Every tool call must execute as an authenticated principal and pass the same scoped domain authorization as the underlying control-plane operation. MCP adds an extra capability layer that can be stricter than REST, but it must never grant broader access.

Required checks:

1. Authenticate the principal.
2. Resolve route scope and realm from the request.
3. Authorize against Fitz route permissions and the principal's separate route-family authority. All-family tools require wildcard family authority. Queue detail/timeline requests with an explicit `queue_family` require access to that family; supplying `queue_family` on another domain does not scope its read. Restricted principals receive `ScopeDenied` for unscoped reads, and denials are audited.
4. Authorize against MCP capability policy.
5. Enforce argument validation and response-size budget.
6. Execute through shared control-plane read models or approved admin commands.
7. Record an audit entry for the decision.

## Capability Classes

| Capability | Intended use | Mutation authority |
| --- | --- | --- |
| `mcp.summary` | bounded health and count summaries | none |
| `mcp.inspect` | bounded resource detail and recent operational facts | none |
| `mcp.explain` | explanation over bounded facts | none |
| `mcp.mutate` | limited administrative actions | restricted and explicit |
| `mcp.admin` | sensitive administrative operations | disabled unless explicitly allowed |

## Implementation Rules

- Prefer existing admin read models.
- Keep tools bounded by route scope, result size, and operation cost.
- Do not add MCP-only bypasses around REST, UI, or admin authorization.
- Do not expose unbounded scans or ad hoc analytics.
- Keep mutation tools opt-in and more restrictive than equivalent REST operations.
- Audit both allowed and denied calls.

The parity tests in [../../tests/mcp_parity.rs](../../tests/mcp_parity.rs) verify that the current MCP read tools mirror their REST control-plane sources.

## Current registry audit retention

The Rust registry retains the most recent 1,024 audit records per execution
context. Clones share that bounded buffer and its saturating eviction counter,
exposed by `dropped_audit_records()`. Readback returns records in invocation
order. Retention is process-local and ends when the last context clone is
released; it is not durable operating evidence or a remote audit exporter.

Each stored string field is capped at 512 UTF-8 bytes. Control characters are
replaced with spaces. Arguments are represented only as `provided`, `absent`,
or `redacted`; the registry does not serialize argument values into audit
records. Unknown tool names are stored as `unknown`. Validation failures and
handler failures receive fixed outcome labels. Authorized/denied resource
scope and provisioned principal names remain bounded operational identifiers.

Read-only calls continue when the buffer is full, evicting its oldest record.
This policy does not authorize administrative actions: the future action
admission path must require an audit sink that can accept its mandatory record
and fail closed when that acceptance is unavailable. No mutations or MCP HTTP
transport are enabled by this retention change.
