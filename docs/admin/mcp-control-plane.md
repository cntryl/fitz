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

## Protocol catalog foundation

The protocol dependency is pinned to `rmcp = 3.5.0`. Fitz's contract selects
`2026-07-28` as the primary revision and `2025-11-25` as explicit compatibility.
The SDK defines different lifecycles: the primary revision does not initialize;
compatibility initializes before operational requests. This dependency and
catalog do not by themselves mount an HTTP endpoint or implement OAuth.

`McpToolRegistry::protocol_tools()` preserves the five existing tool names and
returns them in lexicographic order. Input schemas describe the current registry
arguments; output schemas derive from shared REST DTOs, including all seven
resource-detail variants and resource timelines. Read-only annotations are hints,
not authorization. Every invocation still checks its authenticated principal,
route permissions, route-family authority and capability before collecting data.

Catalog metadata records REST source, authority, snapshot freshness, pagination,
and the current budget enforcement. Encoded registry result bytes and timeline
result-event counts are bounded. Collection scans and hard runtime deadlines are
not yet bounded by these descriptors, and the complete protocol envelope still
needs a transport-level budget. Do not use the candidate 50/100/200ms values as
latency promises. Current `queue_family` scopes Queue only; other domains and
global tools require wildcard family authority until their scoped builders ship.
