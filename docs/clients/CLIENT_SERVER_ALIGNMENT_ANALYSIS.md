# Client Documentation vs Server Implementation — Alignment Analysis

**Purpose:** Analysis of client docs vs server. Section 3 gaps have been resolved via doc and AC updates; use §4–5 for follow-up verification.

**Sources compared:**
- **Client docs:** `CLIENT_SPEC.md`, `CLIENT_ACCEPTANCE_CRITERIA.md`, `CLIENT_IMPLEMENTATION_GUIDE.md`, `CONNECTION_FLOW.md`
- **Server:** Fitz broker codebase (`src/` — protocol, session, domains, api)

**Date:** February 20, 2026

---

## 1. Summary

| Area | Status | Notes |
|------|--------|--------|
| Connection & auth | Aligned | Ports, CONNECT, unauthenticated close reason match |
| Message type ranges & codes | Aligned | KV 100–108, Queue 200/202–204, RPC 300–304, Lease 400–403, Notice 500–504, Stream 600–609, Schedule 700–705 |
| Mux channel mapping | Doc vs impl nuance | Spec uses exact type ranges; server uses 100–199, 200–299, etc. (no conflict) |
| Error codes | Aligned | All domain error code tables match `src/protocol/error_codes.rs` |
| TLV duplicate tags | Aligned | Server rejects duplicate tags; AC-ERROR-001 matches |
| Lease wire format | Aligned | Server encodes `response_type` (0–3) + fencing_token for ACQUIRE; QUERY/RENEW/RELEASE match CLIENT_SPEC |
| Schedule LIST response | Aligned | CLIENT_SPEC unified to `has_entry`; SUBSCRIBE (703) response clarified to status=0 only |
| Queue ENQUEUE_BATCH (201) | Aligned | CLIENT_SPEC notes that 201 is reserved and servers may reject until defined |

---

## 2. What Matches (No Action Needed)

### 2.1 Connection & Authentication

- **Ports:** `prelude`: HTTP/WS 4090, TCP 4091 — matches AC-CONN-001.
- **CONNECT (msg_type 1):** Session layer requires CONNECT first; rejects other frames with close reason `"unauthenticated: connect required"` — matches AC-CONN-005.
- **CONNECT failure:** Manager returns `IngressDecision::Close("connect failed: {}", e)`; transport uses that as close reason — matches AC-CONN-003 ("connect failed: <reason>").
- **Anonymous mode:** `RuntimeIngress::new(auth_required: bool)` and session creation paths support unauthenticated sessions when auth not required — consistent with AC-CONN-004.

### 2.2 Message Types (Wire Numbers)

| Domain | Spec | Server codec | Match |
|--------|------|-------------|-------|
| KV | 100–108 | `kv_codec::msg_type` 100–108 | Yes |
| Queue | 200, 202, 203, 204 | `queue_codec::msg_type` 200, 202, 203, 204 | Yes (201 reserved in spec only) |
| RPC | 300–304 | `rpc_codec` 300–304 | Yes |
| Lease | 400–403 | `lease_codec` 400–403 | Yes |
| Notice | 500–504 | `notice_codec` 500–504 | Yes |
| Stream | 600–609 | `stream_codec` 600–608 (+ 609 NOTIFY server→client) | Yes |
| Schedule | 700–705 | `schedule_codec` 700–704; 705 SCHEDULE_NOTIFY sent by server | Yes |

### 2.3 Mux / Channel Ranges

- **Spec (CONNECTION_FLOW):** 100–108 KV, 300–304 RPC, 500–504 Notice, etc.
- **Server (`mux.rs`):** 100–199 → Pub (KV), 200–299 → Sub (Queue), 300–399 → Rpc, 400–499 → Lease, 500–599 → Pub (Notice), 600–699 → Sub (Stream), 700–799 → Internal (Schedule).

Ranges are broader on the server; unknown types in a range still hit the domain codec and get "Unknown operation". No conflict for defined types.

### 2.4 Error Codes

- **`src/protocol/error_codes.rs`** matches the tables in CLIENT_ACCEPTANCE_CRITERIA and CLIENT_SPEC:
  - KV 1001–1011, Stream 2001–2011, Notice 3001–3009, Queue 4001–4009, Lease 5001–5009, RPC 6001–6009, Schedule 7001–7009.
- Stream: spec 2010 = ERR_INVALID_SUBSCRIPTION_PATTERN, 2011 = ERR_SUBSCRIPTION_LIMIT — server same.

### 2.5 TLV and Parse Errors

- **Duplicate TLV tags:** `tlv.rs` rejects duplicate tags with `TlvError::DuplicateTag` and connection is closed — matches AC-ERROR-001 ("Duplicate TLV tags are NOT permitted").
- **Frame size / 65535:** Not re-verified in this pass; spec and AC-PERF-001 call out limits; assume follow-up if you change limits.

### 2.6 Schedule NOTIFY (705)

- Server sends SCHEDULE_NOTIFY (705) to subscribers (`boot/domains.rs`, `schedule_codec::encode_schedule_notify`).
- Ingress rejects client-sent 705 as server-only — consistent with spec.

### 2.7 Lease Request (Wire)

- Spec: route, owner_id, ttl_secs, optional wait_seconds for ACQUIRE; similar for RENEW/RELEASE/QUERY.
- Server `lease_codec::parse_*`: matches CLIENT_SPEC.

### 2.8 Lease Response (Wire) — Aligned

- **ACQUIRE success:** Server encodes `[u8] status=0`, `[u8] response_type` (0=Acquired, 1=AlreadyHeld, 2=Queued, 3=AlreadyQueued), `[u64 BE] fencing_token` via `lease_codec::encode_domain_response` (see `acquire_response_type` in `src/protocol/lease_codec.rs`).
- **RENEW success:** status=0, new_fencing_token (u64).
- **RELEASE success:** status=0 only.
- **QUERY success (free):** status=0, has_holder=0, pending_waiters=0.
- **QUERY success (held):** status=0, has_holder=1, owner_id, ttl_remaining_secs, pending_waiters.
- **Errors:** status=1, error_len, error_msg. All match CLIENT_SPEC.

---

## 3. Gaps and Mismatches — Resolved

### 3.1 Schedule LIST / SUBSCRIBE

- **LIST:** CLIENT_SPEC now uses `has_entry` consistently for the LIST response sentinel; server already used `has_entry`. Duplicate "has_schedule_id" wording removed.
- **SUBSCRIBE (703):** Spec updated to describe response as status=0 only (matching server); semantics clarified for NOTIFY (705) matching.

---

### 3.2 Queue Message Type 201

- CLIENT_SPEC registry table now notes that 201 (ENQUEUE_BATCH) is reserved and that servers may reject it until defined.

---

### 3.3 RPC Timeout Error Code

- AC-RPC-003: "No workers registered" scenario now expects error code `6004` (ERR_ROUTE_NOT_REGISTERED), matching the error code table.
- AC-ERROR-003: Retryable entry for 6004 updated to "ERR_ROUTE_NOT_REGISTERED; no workers for route or timeout before any reply".

---

## 4. Areas Not Fully Verified (Follow-Up)

- **KV transaction scope (AC-KV-009):** Server enforcement of "transaction began with resource X, operation on resource Y rejected" — not traced in this analysis.
- **Stream expected_offset (AC-STREAM-013):** Server rejection with status=1 and message containing "conflict" — not traced.
- **Schedule LIST "realm" scope:** Spec says "lists all schedules in current realm"; server list implementation and realm filtering not verified.
- **Exact wire layout for every domain:** Only selected request/response shapes were compared (e.g. Lease ACQUIRE response, Schedule LIST sentinel). Full field-by-field wire verification was not done.
- **Max frame size and 65535-byte TLV value:** Config and enforcement not re-checked.

---

## 5. Recommendation Summary

| Item | Recommendation |
|------|----------------|
| Lease ACQUIRE response | **Done:** Server encodes `response_type` (0–3) + fencing_token. |
| Schedule LIST / SUBSCRIBE | **Done:** CLIENT_SPEC unified to `has_entry`; SUBSCRIBE response clarified. |
| Queue 201 | **Done:** CLIENT_SPEC notes 201 reserved and servers may reject. |
| RPC 6004 vs 6001 | **Done:** AC-RPC-003 and AC-ERROR-003 aligned to 6004 (ERR_ROUTE_NOT_REGISTERED) for no-workers scenario. |
| Other items (§4) | Consider targeted verification (KV scope, Stream conflict, Schedule realm, wire layouts, frame limits) in a later pass. |

---

## 6. Document References

- Acceptance criteria: `docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md`
- Wire protocol: `docs/clients/CLIENT_SPEC.md`
- Implementation guide: `docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md`
- Connection flow: `docs/clients/CONNECTION_FLOW.md`
- Server error codes: `src/protocol/error_codes.rs`
- Server mux: `src/protocol/mux.rs`
- Server codecs: `src/protocol/*_codec.rs`
- Session/auth: `src/session/manager.rs`, `src/session/session.rs`

---

**Last updated:** February 20, 2026
