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
