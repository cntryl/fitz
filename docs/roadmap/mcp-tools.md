# MCP Tools Authorization and Safety Model

## Status

Draft

## Summary

Fitz should expose MCP as an AI-facing control-plane interface for safe system observation and limited assisted operations. MCP is not a privileged backdoor, not a new domain, and not a separate business authorization system. It is another interface layer over the same control-plane capabilities already exposed through REST and the UI.

MCP tool calls must execute as the authenticated principal and inherit the same realm and resource scoping, the same underlying domain permissions, and the same or stricter safety constraints as REST. MCP may be stricter than REST, but it must never be broader.

MCP adds one extra policy layer: tool capability policy. That layer constrains which AI-facing tool shapes a principal may invoke, but it does not replace business authorization.

The core design goal is simple: AI gets a smarter interface, not deeper access.

## Motivation

MCP can make Fitz materially easier to operate. It gives AI systems a safe way to answer bounded questions such as:

- what is unhealthy right now?
- which queues are growing?
- what changed recently for this resource?
- why is RPC latency up?
- which consumers are disconnected?

That becomes a strong product feature only if MCP remains an interface layer rather than a deeper execution surface.

Without explicit constraints, MCP risks becoming:

- a policy bypass
- an expensive ad hoc query engine
- a hidden source of operational load
- a mutation path with weak human oversight
- a dumping ground for AI-specific scope creep

## Goals

- Allow AI systems to inspect Fitz using safe, bounded tools.
- Reuse the same authorization model as REST and the UI.
- Prevent MCP from granting broader access than existing control-plane APIs.
- Ensure observational queries cannot negatively impact system performance.
- Support auditability of all MCP activity.
- Keep MCP as an interface layer, not a new domain.

## Non-Goals

- Build an AI workflow platform inside Fitz.
- Build an agent runtime inside Fitz.
- Add privileged AI-only access paths.
- Support arbitrary unbounded analytics or freeform scans.
- Make MCP a replacement for the core SDK or wire protocol.
- Invent a separate business permission system for AI tools.

## Core Model

### Interface Placement

Fitz exposes three primary access modes:

| Interface | Consumer | Purpose |
| --- | --- | --- |
| SDK or wire protocol | applications | primary integration and data-plane access |
| REST | humans and UI | operational control plane |
| MCP | AI systems | system understanding and assisted operations |

MCP belongs to the control plane. It should sit on top of the same safe operational surfaces described in [../admin/admin-api.md](../admin/admin-api.md), not beside them.

### Core Rules

- MCP is not privileged.
- MCP is an interface, not a domain.
- A tool is not a permission.
- Observation must be bounded.
- REST, the UI, and MCP should share the same safe control-plane contract.
- No MCP tool may exceed the worst-case cost already acceptable for the control plane.

## Authorization And Enforcement

Every MCP tool call should pass through the following stages:

1. authenticate the principal
2. resolve realm and resource scope
3. validate tool input
4. map the tool to required underlying domain actions
5. authorize against existing scoped domain permissions
6. authorize against MCP tool capability policy
7. apply cost and safety guardrails
8. execute using safe control-plane APIs or read models
9. emit an audit record

> MCP tool calls execute as the authenticated principal and are authorized against the same scoped domain permissions as REST. MCP adds an additional capability layer to constrain AI-facing operations. No MCP tool may grant broader data access or mutation authority than the underlying control-plane contract.

### Identity And Scope

MCP clients must authenticate as a concrete principal:

- user
- service principal
- delegated session

Anonymous MCP access is not allowed. Every tool call must carry a resolvable identity for authorization and audit.

Authorization must remain scoped across:

- principal
- action
- realm
- domain
- resource, when applicable

Examples:

- `queue.read` on a specific queue
- `stream.read` within a specific realm
- `kv.read` on a specific table or namespace
- `notice.read` on subscription metadata within a specific realm

MCP must not flatten scoped authorization into broad read access.

### Tool-To-Permission Mapping

Tools must map to existing Fitz domain actions instead of inventing separate business permissions.

| MCP tool | Underlying permissions |
| --- | --- |
| `get_system_health` | `system.read` |
| `list_queues` | `queue.read` |
| `get_queue_health` | `queue.read` |
| `list_streams` | `stream.read` |
| `read_stream_window` | `stream.read` |
| `list_subscriptions` | `notice.read` |
| `list_rpc_routes` | `rpc.read` |
| `query_kv_table` | `kv.read` |
| `purge_queue` | `queue.purge` |

The following are not acceptable business capabilities:

- `mcp.use.get_queue_health`
- `mcp.use.find_backpressure`
- `mcp.use.explain_stream`

Those names describe tool entry points, not the underlying domain actions they reach.

## Capability Policy

After domain authorization succeeds, Fitz should apply a second layer: MCP tool capability policy.

This layer constrains which types of AI-facing tools a principal may invoke.

| Capability class | Purpose | Typical examples | Default stance |
| --- | --- | --- | --- |
| `mcp.summary` | bounded summaries, health, counts, top offenders | system health, queue depth summary, disconnected consumers | broadly available to authorized readers |
| `mcp.inspect` | bounded detail lookup and recent history | get queue details, get stream details, list recent events for one resource | available to authorized readers with the appropriate scope |
| `mcp.explain` | AI interpretation over bounded facts | explain queue growth, explain consumer lag | allowed wherever inspect is allowed |
| `mcp.mutate` | limited write operations | purge queue, replay stream, disconnect client, pause consumer | restricted |
| `mcp.admin` | sensitive or dangerous operations | broad replay, destructive administrative actions, realm-wide disruptive operations | disabled unless explicitly allowed |

A tool invocation must satisfy both of the following:

- the required scoped domain permission
- the required MCP capability class

This adds defense in depth without creating permission sprawl.

## Safety And Cost Model

> Observing the system must not negatively impact system performance.

MCP tools should only expose operations whose worst-case cost is already acceptable for the control plane.

### Required Constraints

MCP tools must be:

- bounded
- paged
- sampled where appropriate
- cached where appropriate
- backed by cheap indexes or read models
- explicitly cost-limited
- timeout-bounded
- degradation-aware

### Preferred Data Sources

MCP should preferentially use:

- control-plane read models
- materialized summaries
- bounded indexes
- rolling counters
- recent-window metadata

### Prohibited Query Shapes

MCP tools must not rely on:

- full scans across large keyspaces
- deep history traversal by default
- arbitrary cross-domain joins in hot paths
- unbounded wildcard queries
- live recomputation of expensive operational analytics
- ad hoc wide-fanout execution
- raw internal storage scans
- domain hot paths

### Shared Control-Plane Contract

REST, the UI, and MCP should share the same safe operational surfaces whenever possible.

That means:

- the UI and MCP should read from the same control-plane APIs or read models
- both should inherit the same scoping rules
- neither should depend on private storage details

If a control-plane surface is not safe enough to expose to MCP, the right fix is to harden the shared control-plane surface or add a proper read model, not to create an MCP-only bypass.

## Mutation Policy

MCP should be read-heavy and write-light.

Recommended stance:

- read operations are allowed where bounded and authorized
- write operations are restricted
- dangerous operations are disabled by default or require elevated approval

Even when REST supports a mutation, MCP may still be stricter.

Example:

- the UI may allow queue purge after an explicit operator flow
- MCP may only surface the option, or require confirmation and higher policy to execute it

Mutation tools should support:

- stronger role requirements
- explicit confirmation
- narrow resource targeting
- optional reason capture
- enhanced audit logging

## Audit Requirements

Every MCP tool call must produce an audit record including:

- principal
- tool name
- capability class
- mapped underlying action or actions
- target realm and resource scope
- input parameters or a normalized argument summary
- allow or deny decision
- result summary
- execution cost metrics
- whether the operation was read or mutate
- whether human confirmation was required or provided

This is mandatory because AI-originated requests can be higher volume and less predictable than direct REST interactions.

## Tool Declaration Model

Each MCP tool should declare metadata such as:

- tool name
- purpose
- required underlying actions
- required capability class
- supported scopes
- maximum cost envelope
- timeout
- pagination or window limits
- audit level
- whether human confirmation is required

Example declaration for a bounded read tool:

```text
tool: get_queue_health
purpose: Return a bounded queue health summary
requires: [queue.read]
capability: mcp.summary
scope: queue or realm
cost: bounded
timeout: 1s
audit: standard
confirmation: no
```

Example declaration for a mutation tool:

```text
tool: purge_queue
purpose: Remove queued items from a specific queue
requires: [queue.purge]
capability: mcp.mutate
scope: specific queue only
cost: bounded
timeout: 5s
audit: high
confirmation: yes
```

## Failure Behavior

When a tool invocation exceeds bounds, Fitz should fail safely.

Preferred behaviors:

- require narrower scope
- return summary instead of raw detail
- truncate to a bounded result size
- return partial results with explicit limits noted
- reject unbounded queries outright

Fitz should never silently escalate cost because a question was phrased vaguely.

## Anti-Patterns

- Tool-name authorization only: this hides what data and actions a tool actually reaches.
- Broad AI reader roles: do not create a super-reader role just because AI needs context.
- Hidden tool fanout: a tool that looks small but touches many resources must be treated according to its effective reach.
- Mutation parity by default: REST supporting an operation does not imply MCP should support it equally.
- Unbounded observability: MCP is not an ad hoc analytics engine.

## Consequences

Positive:

- coherent security model
- no duplicated permission system
- clear interface boundaries
- safer AI integration
- bounded operational cost
- strong auditability

Negative:

- some AI requests will need narrowing
- not every question can be answered live
- some analyses require maintained read models or summaries
- mutation flows may feel stricter than direct UI interactions

These tradeoffs are intentional.

## Open Questions

- Should all `mcp.mutate` tools require human confirmation, or only a subset?
- Should some tools be limited to delegated interactive sessions instead of service principals?
- Should capability policy attach to roles, clients, or both?
- Should explain tools be metered separately from inspect tools?
- What should the default bounded recent-history window be for each domain?

## Initial Recommendation

Adopt the following defaults:

- MCP is enabled for bounded read tools only.
- Summary, inspect, and explain tools ship first.
- Mutation tools are opt-in and require explicit policy.
- Dangerous tools are disabled by default.
- All tools execute against existing control-plane surfaces.
- Every tool call is audited.
- No tool is allowed unless its worst-case cost is already acceptable for REST.

## Concise Rule Set

- same auth model as REST
- same scoped permissions as REST
- extra MCP capability policy layer
- bounded reads only by default
- write tools restricted
- dangerous tools disabled by default
- no unbounded scans
- no hidden fanout
- no privileged AI backdoor
- full audit trail for every call
