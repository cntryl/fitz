# KV and Stream Domain Snapshots

Fitz exposes route-pattern snapshots for committed KV values and readable
Stream history through `Runtime::capture_kv_snapshot`,
`Runtime::restore_kv_snapshot`, `Runtime::capture_stream_snapshot`, and
`Runtime::restore_stream_snapshot`. Selectors name the domain and route family
explicitly; the family is never inferred from the realm or route pattern.

Artifacts are versioned JSON envelopes with a SHA-256 checksum. Restore parses
and validates the complete artifact, selector, family, counts, and contents
before it sends a mutation command. Resource identities must be exact concrete
routes under the selected domain's grammar; wildcard patterns are allowed only
in selectors. A malformed resource invalidates the complete artifact, even when
its checksum is valid or other resources are valid. Keep the encoded bytes with
the application backup that owns them; Fitz does not schedule, upload, or inventory artifacts.

## KV

Capture reads committed rows in one family-scoped read transaction. Exact
selectors include an empty resource when no values exist. Realm-wide selectors
include only resources with committed user rows. Inventory metadata is not a
source of capture truth because it can lag committed writes.

Restore runs on the selected family actor and replaces each selected concrete
resource in its own storage transaction. Resources created after capture are
cleared even when they are absent from the artifact. A failure leaves earlier
resource transactions committed; the error identifies completed routes. There
is no cross-resource atomicity claim.

Restore does not wait for client transactions. Before it mutates anything, it
expires idle resource locks the same way a read-write `BEGIN` does, then
rejects the whole restore with an error naming the route if any target
resource is still locked by an open read-write transaction. Retry after that
transaction commits, rolls back, or expires.

After each resource transaction commits, restore routes one live KV watch
notification for that resource route, the same way a client commit does. The
mutation count is the number of rows the restore deleted plus the number it
wrote; a resource with neither produces no notification. Like other KV
notifications, these are live and ephemeral: watchers that are not connected
during the restore do not receive them later.

## Stream

Capture runs on the selected family actor. It records each selected resource's
exclusive committed resource watermark and the readable event bodies and
metadata below that watermark. The artifact preserves per-resource event order.
Expired events omitted by normal Stream reads are not recreated.

Restore requires every matching destination resource to have no committed
history before replay starts. It writes events through the Stream storage
commit path, assigning new resource, area, realm, and family offsets. Source
offsets, watermarks, cursors, append sessions, and subscriptions are not
restored. A replay failure reports completed routes and progress within the
resource that failed; earlier commits are not rolled back.

KV and Stream captures are separate operations and do not form one consistent
cross-domain cut. These APIs are domain data movement primitives, not a
whole-platform backup, cloud-provider backup, or recovery orchestration system.
