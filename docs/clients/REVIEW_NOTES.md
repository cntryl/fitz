# Fitz Client Docs — Review Notes

All issues identified and resolved. This document serves as the changelog for the corrected doc set.

---

## Resolutions Applied

### CLIENT_SPEC.md

| # | Issue | Resolution |
|---|-------|------------|
| 1 | Duplicate acceptance test sections (Stream–Schedule listed twice) | Removed duplicate block |
| 2 | Corrupted Queue and Schedule route shape tables | Rebuilt tables with clean markdown |
| 3 | KV BEGIN response missing status byte | Added `[u8] status` prefix to match universal response convention |
| 4 | MessageType encoding ambiguous (1 vs 2 bytes) | Added definitive decoder/encoder pseudocode in Type Encoding Rules. Types ≤ 0xFE = 1 byte, > 0xFE = `0xFF` escape + u16 BE. Note added that wire examples use 2-byte for readability |
| 5 | KV durability values swapped in wire format | Corrected to `0=Buffered, 1=Sync` (was backwards) |
| 6 | ENQUEUE_BATCH has no wire format | Marked as `Reserved` in verb table and constants registry |
| 7 | Lease QUERY missing from route shape table | Added QUERY row |
| 8 | Lease error codes used 4xxx instead of 5xxx | Corrected to 5001–5004 |
| 9 | "Empty on success" response ambiguous for KV ops | All KV responses now explicitly show `[u8] 0` status byte for success, full `[u8 1][u32 error_len][bytes error_msg]` for errors |
| 10 | Notice PUBLISH had response payload | Changed to fire-and-forget — no response frame. Added semantics note |
| 11 | Stream response `data` fields unexplained | Added note: broker-defined opaque bytes, clients MUST parse past but SHOULD NOT interpret |
| 12 | RPC worker delivery format undocumented | Added "REQUEST Delivery" section — broker forwards same REQUEST frame to worker |
| 13 | Schedule execution behavior undefined | Added Execution Model section — broker performs `target_operation` on `target_resource` internally |
| 14 | Backpressure signal format undefined | Replaced vague language with concrete error codes per domain (6003, 4005). No separate backpressure frame. Notice drops silently |
| 15 | CONNECT timeout required 5s wait before sending | Changed to send-immediately pattern. Client sends CONNECT then domain requests right away; auth failure discovered via connection close |

### CLIENT_ACCEPTANCE_CRITERIA.md

| # | Issue | Resolution |
|---|-------|------------|
| 16 | AC-CONN-002 referenced `route_family` JWT claim | Replaced with standard IdP claims (`sub`, `iss`, `aud`, `scopes`, `exp`, optional `tid`/`tenant_id`) |
| 17 | AC-KV-005 wrong error code (1003 → 1006) | Corrected to `1006 ERR_KEY_EXISTS` |
| 18 | AC-KV-010 wrong error code (1009 → 1001) | Corrected to `1001 ERR_UNAUTHORIZED` |
| 19 | AC-KV-011 referenced non-existent error code | Changed to generic "server returns error" with note that specific code is broker-defined |
| 20 | AC-RPC-005 used "ReplyChunk" (not a real verb) | Changed to RESPONSE with `sequence` and `stream_end` fields |
| 21 | AC-QUEUE-006 used string token instead of u64 | Changed to `<invalid_u64>` |

### CLIENT_IMPLEMENTATION_GUIDE.md

| # | Issue | Resolution |
|---|-------|------------|
| 22 | Python MessageType enum had wrong COMMIT/ROLLBACK codes | Corrected: `COMMIT=101, ROLLBACK=102` |

### CONNECTION_FLOW.md

No changes needed — already consistent with corrected spec.

---

## Design Decisions Made

These were open questions resolved with idiomatic answers:

1. **Notice PUBLISH is fire-and-forget** — no response frame, no error, matches best-effort semantics
2. **SUBSCRIBE/UNSUBSCRIBE still return responses** — client needs `subscription_id` for local multiplexing
3. **All KV responses use the universal status byte convention** — no exceptions
4. **ENQUEUE_BATCH is reserved, not removed** — wire code 201 stays allocated for future use
5. **Stream response `data` is opaque** — clients parse past it, don't interpret it
6. **Workers receive the raw REQUEST frame** — broker forwards same MessageType=302 frame
7. **Schedule execution is broker-internal** — broker acts as internal client performing the target operation
8. **Backpressure is signaled via domain errors** — no special frame, uses existing error codes
9. **CONNECT is non-blocking** — client sends domain requests immediately, doesn't wait for ACK
10. **MessageType encoding is definitively variable-length** — with pseudocode for both encoder and decoder
