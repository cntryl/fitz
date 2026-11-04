# Stream DROP

`DROP` (message type **610**) is a standalone destructive operation. It removes
committed history in one selected scope within the session's assigned
`RouteFamily`. It does not use an append-session ID.

## Request and response

```text
[u32 BE] selector_len
[bytes]  selector (UTF-8)
[u8]     persistence (0 = Buffered, 1 = Sync)
```

Persistence is required. No additional fields or trailing bytes are accepted.
Buffered and Sync use the same configured local or cloud persistence boundaries
as Stream COMMIT; background configuration does not downgrade Sync.

A successful response is exactly `[u8 status=0]`. Errors use the common
[error envelope](error-codes.md). A timeout or indeterminate backend outcome
does not establish that deletion failed; clients must not blindly retry.
A repeated DROP also removes history appended since the earlier request.

## Selectors and authorization

Only these selectors are accepted; the `stream://` prefix may be omitted:

| Selector | Selected history | Required write-permission coverage |
| --- | --- | --- |
| `stream://{realm}/*` | Every resource in the realm | `stream://{realm}/*/*` |
| `stream://{realm}/{area}/*` | Every resource in the area | `stream://{realm}/{area}/*` |
| `stream://{realm}/{area}/{resource}` | One resource | The exact resource |

Realm, area, and resource components are literal. Other wildcard combinations,
`**`, partial wildcards, operation suffixes, reserved Stream components, empty
segments, foreign schemes, and control characters are rejected. The expanded
authorization route must fit the normal route byte limit.

One granted write-permission pattern must contain the complete selected set.
A narrower resource permission cannot authorize area deletion, and a narrower
area permission cannot authorize realm deletion. Read permission is insufficient.
DROP's realm shorthand does not extend permission or replay-selector grammar.
`realm` remains an application-defined namespace, independent of `RouteFamily`.

## Destructive semantics

Deletion is atomic across the Stream replay views: subsequent resource, area,
realm, global, and filtered reads omit the removed records. Surviving history
retains its original offsets; wider scopes can therefore contain gaps.

Offsets reset only where the entire scope is removed:

| DROP scope | Resource offsets | Area offsets | Realm offsets | Family-global offsets |
| --- | --- | --- | --- | --- |
| Resource | Selected resource resets | Preserved | Preserved | Preserved |
| Area | Resources in area reset | Selected area resets | Preserved | Preserved |
| Realm | Resources in realm reset | Areas in realm reset | Selected realm resets | Preserved |

A recreated resource begins at resource offset zero. Applications must discard
old revisions, checkpoints, and cursors for reset scopes. The broker does not
add generation tokens or distinguish a previous incarnation's offset from a
recreated stream's offset. A global cursor's captured watermark does not retain
records removed by DROP.

DROP rejects any selected resource with an active append session, including an
empty session owned by the caller. Applications close those sessions explicitly
before requesting deletion. Unrelated append sessions remain active. An empty
scope succeeds, and resources can be recreated after deletion.

Live subscriptions remain registered. Notifications are ephemeral commit hints;
an older hint can still arrive around a deletion. DROP does not enumerate deleted
records or provide a durable deletion-notification feed. Clients reconcile by
reading current retained history.

Logical deletion completes at the selected persistence boundary. Physical space
reclamation is eventual and uses storage range deletion and compaction; success
does not mean that every historical byte has already been reclaimed.
Repeated DROP requests retain forward progress for already queued reclamation.

Snapshot export after deletion includes retained history only. Snapshot restore
replays records into an empty destination and does not preserve original offsets.

The first DROP advances that family's Stream storage marker to D5 so an older
broker cannot silently ignore deletion fences. The new broker reads existing D4
history. Rolling back to an older broker requires its matching pre-DROP store
snapshot rather than opening a store that has used DROP.
