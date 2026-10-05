# MCP Control Plane

Fitz exposes an opt-in Model Context Protocol endpoint over the existing HTTP
listener. MCP uses the same admin read models, route permissions, and shared
admin command functions as REST. It is not a Fitz domain or a separate source
of authorization.

## Enable the endpoint

The endpoint is disabled unless `FITZ_MCP_HTTP_ENABLED=true`. When enabled,
configure these values:

| Variable | Meaning |
| --- | --- |
| `FITZ_MCP_OAUTH_ISSUER` | HTTPS issuer expected in bearer tokens. |
| `FITZ_MCP_OAUTH_AUDIENCE` | Exact audience expected in bearer tokens. |
| `FITZ_MCP_PUBLIC_URL` | Public HTTPS MCP URL ending in `/mcp`. |
| `FITZ_MCP_OAUTH_PUBLIC_KEY_FILE` | PEM RSA public key used to verify RS256 tokens. |
| `FITZ_MCP_ALLOWED_ORIGINS` | Optional comma-separated HTTPS Origin allowlist. Defaults to the origin in `FITZ_MCP_PUBLIC_URL`. |
| `FITZ_MCP_DOCUMENTATION_URL` | Optional HTTPS link advertised in protected-resource metadata. |

Terminate TLS at Fitz or at a trusted reverse proxy. The proxy must preserve a
single canonical `Host` authority. Fitz checks it against the configured public
host and effective HTTPS port. Present `Origin` headers must match the
configured allowlist; requests without `Origin` are accepted for non-browser MCP
clients. The same checks cover `/mcp` and protected-resource metadata.

`GET /.well-known/oauth-protected-resource/mcp` returns the resource URL,
authorization server issuer, supported scopes, bearer method, and optional
documentation URL. The OAuth authorization server issues tokens; Fitz validates
them and does not host an authorization or token endpoint.

The [pinned Keycloak setup](mcp/oauth-provider.md) supplies an import for an
S256 PKCE human client and a separate machine client with a narrow Queue read
grant. It describes signing-key provisioning and the explicit restart required
for key rotation.

## Bearer token contract

Fitz accepts bounded RS256 bearer tokens only. Signature, issuer, audience,
expiry, not-before, issue time, subject, role, route-family grants, permissions,
capabilities, and OAuth scopes are validated before a request reaches MCP.
Tokens must carry these claims:

```json
{
  "iss": "https://identity.example.test",
  "sub": "operator-17",
  "aud": "https://fitz.example.test/mcp",
  "iat": 1791100800,
  "exp": 1791104400,
  "fitz_role": "admin",
  "fitz_route_families": ["41"],
  "fitz_permissions": ["queue://**#read", "kv://**#read"],
  "fitz_mcp_capabilities": ["summary", "inspect"],
  "scope": "fitz.mcp.read"
}
```

`fitz_route_families` is either `"*"` or a non-empty array of canonical,
provisioned family numbers. `fitz_permissions` contains Fitz route permissions
in `<route>#<access>` form, where access is `read`, `write`, or `*`. Capability
classes are `summary`, `inspect`, `explain`, `mutate`, and `admin`. The matching
OAuth scopes are `fitz.mcp.read`, `fitz.mcp.mutate`, and `fitz.mcp.admin`.
Read and explain capabilities require the read scope; mutation and admin
capabilities require their corresponding scopes. A scope alone grants no Fitz
route permission or route-family authority.

The application-defined `realm` and broker `route_family` are separate values.
Neither is inferred from the other. Global reads require wildcard family
authority and READ permission across every domain. A resource read is checked
against its exact route and family. An explicit family must be provisioned and
allowed by the principal. Omitting a family is available only to a wildcard
principal and permission set.

## Protocol surface

The primary revision is `2026-07-28`, which uses stateless discovery and
per-request protocol metadata. `2025-11-25` remains supported with the
initialize/initialized session lifecycle. Compatibility session IDs are bound
to the bearer-token fingerprint, expire with the token, and are discarded after
one hour idle; at most 4,096 bindings are retained per process.
Each authenticated request reaps at most 32 expired inner sessions. A binding
is released after the inner service accepts its DELETE request.

Read-only clients can list ten tools: global stats, global troubleshooting,
explanation, MCP discovery, sessions, topology, structured metrics, resource
inventory, resource detail, and resource timeline. The authorized catalog exposes
broker and family templates plus resource templates for all seven Fitz
domains. Three static documentation resources describe [domain guarantees](mcp/domain-guarantees.md),
[operational fields](mcp/operational-fields.md), and
[troubleshooting](mcp/troubleshooting.md). Prompts provide global broker
diagnosis, detail and timeline inspection, and a guided diagnosis for each of
the seven domains. Operational resources and prompts require the inspect
capability; static documentation is not broker-specific.

Versioned URIs are `fitz://broker/v1/summary`,
`fitz://family/v1/{route_family}/topology`,
`fitz://resource/v1/{domain}/{route_family}/{realm}/{area}/{resource}`, and
`fitz://docs/v1/{document}`. Encode each application namespace segment separately.
Legacy documentation and resource aliases remain readable without being
advertised. Every operational URI runs the same permission and capability
checks as its corresponding tool; knowing a URI grants no access.

Operational JSON includes `_meta` observation time, evidence ID, source,
freshness, partial and unavailable markers. Prompts require the reader to cite
that evidence, keep observations separate from hypotheses and treat broker
strings as untrusted data. Empty or partial bounded snapshots do not prove
absence or complete history.
Cached indexed projections identify themselves and report unknown source age
when `source_updated_at` is `null`; `observed_at` is the collection time.

The reads mirror the admin REST contracts and current read models. They do not
add data-plane permissions or infer history, ownership, replay, or recovery.
See [domain guarantees](mcp/domain-guarantees.md) for Fitz's domain meanings.

## Resource and execution bounds

| Limit | Enforced value |
| --- | ---: |
| HTTP request body | 64 KiB, checked before RMCP parses JSON |
| HTTP body read deadline | 5 seconds from admission |
| Encoded tool arguments | 16 KiB |
| Concurrent HTTP requests | 32 |
| Concurrent tool executions | 32; a blocking worker holds its permit until it exits |
| Serialized JSON-RPC response envelope | 600 KiB |
| Summary result | 256 items, 64 KiB, 50 ms wait budget |
| Resource detail | 512 items, 128 KiB, 100 ms wait budget |
| Resource timeline | 50 items, 256 KiB, 200 ms wait budget |
| Session collection | 256 items, 512 KiB, 200 ms wait budget |
| Topology | 2,048 items, 512 KiB, 250 ms wait budget |
| Structured metrics | 256 samples, 256 KiB, 200 ms wait budget |

The server rejects results that exceed their item or byte budget and rejects a
protocol envelope that exceeds 600 KiB. Session and metric collections include
`truncated` and `limit` markers when capped. Timeline requests accept a limit
from 1 through 50. Runtime values are wait budgets, not hard preemption: Rust
read-model calls are synchronous, so a timed-out or cancelled worker may finish
in the background. Its permit remains held, and Fitz does not return its late
result to the cancelled request. Admission failures return a retryable busy
response; callers should use bounded backoff.

### Local budget measurement

On 2026-10-04 the explicit debug-build acceptance measurement executed all ten
read tools 20 times each against a local runtime whose cached admin read model
contained 500 RPC worker snapshots. It measured tool execution and encoded tool-result bytes
under concurrent local test load, using cached indexed projections. It did not
include OAuth provider latency or HTTP/network time and does not establish a
hard execution deadline. Reproduce with:

```sh
cargo test --lib should_measure_bounded_operational_reads_without_promising_hard_preemption -- --ignored --nocapture
```

| Tool | p95 (ms) | Largest result (bytes) |
| --- | ---: | ---: |
| `get_global_stats` | 1.506 | 9,097 |
| `get_global_troubleshooting` | 0.537 | 1,368 |
| `inspect_resource_detail` | 0.153 | 972 |
| `inspect_resource_timeline` | 0.158 | 2,483 |
| `explain_global_troubleshooting` | 0.519 | 1,368 |
| `get_mcp_discovery` | 0.081 | 1,601 |
| `get_sessions` | 0.040 | 641 |
| `get_topology` | 5.363 | 69,307 |
| `get_structured_metrics` | 0.216 | 2,947 |
| `list_resource_inventory` | 1.036 | 9,285 |

No read exceeded its item/byte/wait budget in these 200 calls. The largest
elapsed time was 5.514 ms (`get_topology`). These are focused acceptance
observations, not production latency targets or measurements of source age.

MCP audit records retained in memory are bounded to the most recent 1,024
entries and share a bounded buffer across the MCP HTTP listener's callers. Text fields are
capped at 512 UTF-8 bytes and control characters are replaced. Argument values
are not stored. This retention is diagnostic only, not durable evidence.
Fixed-cardinality Prometheus metrics are appended to `/metrics`; they have no
principal, route, family, or tool-name labels. They report requests, denials,
errors, authentication failures, cancellations, overload rejections, in-flight
calls, duration aggregates, and audit evictions.

## Guarded operations

Mutations are disabled unless `FITZ_MCP_MUTATIONS_ENABLED=true`. Enabling them
also requires:

| Variable | Meaning |
| --- | --- |
| `FITZ_MCP_ACTION_TARGET` | Bounded safe identifier for the runtime drain target. |
| `FITZ_MCP_ACTION_AUDIT_FILE` | Absolute path to the mandatory JSONL audit file. |

The audit file is opened without following a symlink, restricted to mode 0600
on Unix, and capped at 16 MiB. Each preview, intent, and outcome record is
synced to disk. If the mandatory audit append fails, Fitz does not start the
command. A full file fails closed; rotate or archive it while MCP mutations are
disabled. Denials also require a durable audit record.

Queue dead-letter replay and purge require an explicit provisioned family,
Queue route WRITE permission, and matching principal family authority. Replay
requires `mutate`; purge also requires `admin`. Runtime drain requires `mutate`
and `admin`, wildcard family authority, and WRITE permission across all seven
domains.

Each operation has a preview tool and a confirmation tool. A preview binds a
one-use challenge to the principal, bearer-token fingerprint, exact action and
target, and the observed state hash. It expires after 60 seconds. Queue targets
include family, Queue route, and message ID. Runtime drain includes the
configured target and observed lifecycle/session state. Confirmation requires
the exact returned target and confirmation phrase; Fitz rechecks authority and
observed state immediately before dispatch. Queue operations use the same
shared admin command as REST. Runtime drain uses the same shared drain command
as REST. A changed target/state requires a fresh preview.

If cancellation or timeout occurs after an action may have started, the result
is indeterminate. Fitz records the indeterminate outcome when possible and
returns a do-not-retry instruction with the operation ID. Inspect the durable
audit record and broker state before taking further action.

## Stdio adapter

`fitz-mcp-stdio` connects local MCP clients to this authenticated remote
endpoint. Configure:

```sh
FITZ_MCP_HTTP_URL=https://fitz.example.test/mcp
FITZ_MCP_BEARER_TOKEN_FILE=/absolute/path/to/token
```

The token file must be a regular non-symlink file with group and other access
disabled on Unix. The adapter negotiates the primary revision and falls back to
the `2025-11-25` initialize lifecycle. For local development only, it accepts
plain HTTP when the endpoint host is loopback. It sends diagnostics to stderr
and reserves stdout for MCP stdio frames.

The adapter forwards cooperative request cancellation upstream for tools,
resources, prompts and discovery. A request canceled before forwarding starts
no remote work. After admission, a canceled confirmed action has an
indeterminate outcome: do not retry it automatically. Remote synchronous
cleanup may continue; the broker's durable audit remains the action-status
authority.

## Tests and changes

The Rust MCP tests verify the shared read contracts, family authorization,
catalog schemas, protocol lifecycle, prompt scope parsing, Host/Origin
normalization, and audit bounds. Remote Streamable HTTP interoperability and
the stdio process adapter are exercised against real RMCP clients. Changes to
the REST sources remain covered by the existing admin route and parity tests.
