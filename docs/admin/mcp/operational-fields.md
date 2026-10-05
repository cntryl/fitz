# MCP operational fields

MCP uses the same admin read models as REST. Values are snapshots from the
current broker process unless a field explicitly names durable domain state.

- `realm` identifies the application namespace from Fitz routes and permissions.
- `route_family` identifies the independent broker routing and isolation family.
- Session lists describe active sessions. A reconnect creates a new session.
- Queue dead-letter facts identify a persisted message and its attempt count;
  routine diagnostics do not return message payloads.
- Stream diagnostics report committed record counts, cursors, and bounded recent
  transitions. A recent transition list is not a complete history export.
- KV diagnostics report current state metadata. They do not return stored values
  unless a separately authorized REST operation explicitly supplies them.
- RPC worker and pending-request facts describe live request/response work. They
  do not imply durable recovery or exactly-once execution.

Metric samples may be truncated at the advertised result limit. The response
includes `truncated` and `limit` fields when that happens.

Tool and operational resource values share `_meta` observation metadata.
`observed_at` names the collection time, `evidence_id` identifies the observation,
and `source` identifies the broker read model. Cached indexed projections set
`cached_projection: true`. Their `source_updated_at` is `null` when the source
does not provide an update timestamp; collection time does not establish the
age of the underlying facts. Read the `freshness` and availability markers
before describing an observation as current. `partial` and `unavailable` report
limits or unavailable sources. An empty bounded collection or omitted field
does not prove that a resource has no history or that an unavailable source is
healthy. Cite evidence identifiers and timestamps when drawing conclusions.

`get_global_stats` reports bounded projection counts and fixed broker counters.
`_meta.unavailable_fields` identifies JSON pointers whose DTO defaults are
placeholders: unavailable KV key totals and operation rate, Stream watermark
lag buckets, and live Schedule execution/subscription/retry/claim-age/failure
inputs. These defaults are unknown measurements. Global diagnostics report
observed pressure as hypotheses; cached projections with unknown source age
cannot establish that the broker is healthy.

Resource reads select the domain, authorized family and concrete resource
before copying rows. Wildcard Notice/RPC registrations are matched against the
requested resource, with a bounded scan and explicit incomplete evidence if
the scan is exhausted. Wildcard operation names cannot be enumerated, and
their aggregate handled counts/latency are unavailable for one resource.
Family-level Schedule pressure is also unavailable for resource timelines.

Flexible Notice/RPC routes are grouped by the first three path components;
operation groups use the first four. Pattern coverage includes a legal finite
suffix within the 64-component route limit. Ambiguous multi-segment wildcard
operation names remain unavailable. Wildcard Notice delivery/publication
counts are also unavailable for a single resource.

Discovery exposes fixed lifecycle flags and configured features. Aggregate
`broker.ready` uses the existing lifecycle flags and seven domain pool
availability checks. Each check reads the pool's atomic flags without
traversing family health rows. One failed family leaves a domain usable while
a sibling remains available; all families failing or pool shutdown makes it
unavailable. Individual family readiness detail is unavailable in Discovery.
Stream names shared by multiple route families require an explicit family for
resource detail/timeline reads so offsets and watermarks stay independent.

Documentation URIs are versioned as `fitz://docs/v1/<name>`. Operational URI
templates are `fitz://broker/v1/summary`,
`fitz://family/v1/{route_family}/topology` and
`fitz://resource/v1/{scheme}/{route_family}/{realm}/{area}/{resource}`.
URI versions identify the resource contract, independently of MCP protocol
revisions. Percent-encode path segments; encoded separators and wildcards are
rejected. Initial unversioned document/resource URIs remain readable aliases.
