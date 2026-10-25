# Resource Limits

Fitz bounds live registrations and transport-owned frame bytes. Other limits
are documented with their owning domain or configuration setting.

## Live Registration Limits

Each registration domain retains at most 1,024 registrations per session,
counting exact and wildcard registrations together. Wildcard registrations are
also capped at 128 per session. Duplicate registrations remain idempotent and
are checked before either cap.

## Transport byte admission

TCP and WebSocket share one frame-byte budget across their listeners. Each
session shares a second budget between incoming frames and outgoing replies
and notifications. Defaults are 64 MiB shared and 8 MiB per session. Each
budget is raised to at least twice `max_message_bytes` when a larger message
limit is configured. Message-count and connection limits also apply.

Admission occurs before queueing a complete frame. A reservation remains held
through processing and transport writes, including retained `Bytes` clones
and slices. Releasing the final owner returns the capacity. Failed shared
admission returns any provisional session reservation immediately.

An incoming response-bearing request refused before dispatch receives
BUSY/NOT_APPLIED when its session is established and the refusal can be queued.
Unacknowledged Notice publishes may be dropped. Required protocol traffic
that cannot be buffered, and required outgoing replies that cannot be encoded
or queued, close the session, leaving outstanding outcomes indeterminate.
Best-effort live notifications can be dropped. Actors never wait for a slow
consumer to free byte capacity.

These bounds cover transport-owned frame bytes. They are not a process RSS
limit: socket/parser buffers, encoding workspaces, storage, and domain state
have separate allocation and capacity boundaries.
