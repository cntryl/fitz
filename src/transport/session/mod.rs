use std::sync::Arc;

use crate::core::engine::EngineHandle;
use crate::protocol::frame as fr;
use crate::storage::RouteFamilyId;
use crate::transport::mux::Muxer;

mod state;
pub use state::SessionState;

/// Convert a tenant name to a route family ID
/// For now, uses a simple hash-based approach
fn tenant_to_route_family(tenant: &str) -> RouteFamilyId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    tenant.hash(&mut hasher);
    let hash = hasher.finish();
    (hash % 256) as RouteFamilyId // Limit to 256 route families
}

/// Register a default channel handler (channel_id) on the given mux.
/// The handler processes FTZ frames for auth, publish, REG (subscribe/unsubscribe), and REQ.
pub async fn register_default_channel(mux: Arc<Muxer>, engine: EngineHandle, channel_id: u32) {
    let (ch_tx, mut ch_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(32);
    mux.register_channel(channel_id, ch_tx).await;

    let state = SessionState::new(mux.clone(), engine.clone(), channel_id);

    // Heartbeat task (TLV: send a DAT heartbeat on the same channel)
    {
        let mux_hb = mux.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                ticker.tick().await;
                let mut p = Vec::new();
                fr::build_tlv(fr::TAG_NOTIFICATION, &[], &mut p);
                let hb = fr::build_frame(fr::FRAME_DAT, 0, channel_id, &p);
                mux_hb.send_on_channel(hb).await;
            }
        });
    }

    tokio::spawn(async move {
        use crate::authz::permissions::{self, Action};
        use crate::protocol::route as route_mod;

        let inflight = state.inflight.clone();
        let permits = state.permits.clone();
        let auth_state = state.auth_state.clone();
        let mux_clone = state.mux.clone();
        let engine_clone = state.engine.clone();
        let ack_delay_ms = state.ack_delay_ms;
        let subs = state.subs.clone();
        let channel = state.channel_id;

        while let Some(frame_bytes) = ch_rx.recv().await {
            if let Some((_ftype, flags, chan)) = read_header(&frame_bytes) {
                // Flow control: enforce ack window if client marks ACK_REQUIRED
                let needs_ack = (flags & fr::FLAG_ACK_REQUIRED) != 0;
                // Try to acquire a permit; if not available, emit ERR 1006 immediately
                let permit_opt = if needs_ack {
                    permits.clone().try_acquire_owned().ok()
                } else {
                    None
                };
                if needs_ack && permit_opt.is_none() {
                    let _ =
                        send_err_chan(mux_clone.clone(), chan, 1006, "flow control", None).await;
                    continue;
                }

                // Spawn per-frame handler to allow overlap and let window enforce concurrent caps
                let inflight_task = inflight.clone();
                let mux_task = mux_clone.clone();
                let engine_task = engine_clone.clone();
                let auth_task = auth_state.clone();
                let permits_task = permits.clone();
                let mut _permit_owned = permit_opt; // moved into task to drop on completion

                // clone subs per-iteration so inner task takes ownership of a separate Arc
                let subs_for_spawn = subs.clone();
                let channel_for_spawn = channel;

                tokio::spawn(async move {
                    // Optional delay to simulate work and keep inflight occupied
                    if needs_ack && ack_delay_ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(ack_delay_ms)).await;
                    }

                    // Get route family from tenant
                    let route_family = {
                        let tenant_opt = auth_task.lock().await;
                        match tenant_opt.as_ref() {
                            Some(tenant) => tenant_to_route_family(tenant),
                            None => crate::storage::DEFAULT_RF,
                        }
                    };

                    if let Ok(parsed) = fr::parse_frame(&frame_bytes) {
                        let mut ack_sent = false;
                        match parsed.header.frame_type {
                            x if x == fr::FRAME_CONN_OPEN => {
                                // Client -> Broker: accept TAG_TOKEN here (HELLO) per SPEC note; negotiate ack_window (future)
                                if let Some(token_b) = fr::find_tlv(parsed.payload, fr::TAG_TOKEN) {
                                    let token = String::from_utf8_lossy(token_b).to_string();
                                    if let Some(tenant) = crate::authz::validate_token(&token) {
                                        let mut g = auth_task.lock().await;
                                        *g = Some(tenant);
                                        // install claim grants if available from mock token
                                        if let Some(claims) =
                                            crate::authz::mock_jwks::validate_mock_token(&token)
                                        {
                                            crate::authz::permissions::install_claim_grants(
                                                g.as_ref().unwrap(),
                                                &claims,
                                            )
                                            .await;
                                        }
                                    } else {
                                        let _ = send_err_chan(
                                            mux_task.clone(),
                                            parsed.header.channel_id,
                                            1001,
                                            "invalid token",
                                            None,
                                        )
                                        .await;
                                    }
                                }

                                // Proposed ack_window
                                if let Some(win_b) =
                                    fr::find_tlv(parsed.payload, fr::TAG_ACK_WINDOW)
                                {
                                    if win_b.len() >= 4 {
                                        let proposed = u32::from_be_bytes([
                                            win_b[0], win_b[1], win_b[2], win_b[3],
                                        ])
                                            as usize;
                                        let max_allowed = crate::config::load().broker.ack_window;
                                        let target = proposed.min(max_allowed).max(1);
                                        // replace semaphore with higher permits if needed
                                        let extra = if target > permits_task.available_permits() {
                                            target - permits_task.available_permits()
                                        } else {
                                            0
                                        };
                                        if extra > 0 {
                                            permits_task.add_permits(extra);
                                        }
                                    }
                                }

                                let ack = fr::build_frame(
                                    fr::FRAME_ACK,
                                    0,
                                    parsed.header.channel_id,
                                    &[],
                                );
                                mux_task.send_on_channel(ack).await;
                                ack_sent = true;
                            }

                            x if x == fr::FRAME_CONN_CLOSE => {
                                // Broker <- Client: Authorization bearer in headers
                                let token = fr::find_tlv(parsed.payload, fr::TAG_TOKEN)
                                    .map(|b| String::from_utf8_lossy(b).to_string());
                                if let Some(token) = token {
                                    if let Some(tenant) = crate::authz::validate_token(&token) {
                                        let mut g = auth_task.lock().await;
                                        *g = Some(tenant);
                                        if let Some(claims) =
                                            crate::authz::mock_jwks::validate_mock_token(&token)
                                        {
                                            crate::authz::permissions::install_claim_grants(
                                                g.as_ref().unwrap(),
                                                &claims,
                                            )
                                            .await;
                                        }
                                        // send ACK for auth
                                        let ack = fr::build_frame(
                                            fr::FRAME_ACK,
                                            0,
                                            parsed.header.channel_id,
                                            &[],
                                        );
                                        mux_task.send_on_channel(ack).await;
                                        ack_sent = true;
                                    } else {
                                        let _ = send_err_chan(
                                            mux_task.clone(),
                                            parsed.header.channel_id,
                                            1001,
                                            "invalid token",
                                            None,
                                        )
                                        .await;
                                        ack_sent = true;
                                    }
                                } else {
                                    let _ = send_err_chan(
                                        mux_task.clone(),
                                        parsed.header.channel_id,
                                        1001,
                                        "missing Authorization",
                                        None,
                                    )
                                    .await;
                                    ack_sent = true;
                                }
                            }

                            x if x == fr::FRAME_PUB => {
                                if let Some(route_b) = fr::find_tlv(parsed.payload, fr::TAG_ROUTE) {
                                    let route_s = String::from_utf8_lossy(route_b).to_string();
                                    if route_s == "control://sys/token/issue" {
                                        if let Ok(pubref) = fr::parse_pub(&parsed) {
                                            let reply_to =
                                                fr::find_tlv(parsed.payload, fr::TAG_ROUTE_REPLY)
                                                    .map(|b| {
                                                        String::from_utf8_lossy(b).to_string()
                                                    });
                                            if let Some(reply_route) = reply_to {
                                                let v: serde_json::Value =
                                                    serde_json::from_slice(pubref.body)
                                                        .unwrap_or(serde_json::Value::Null);
                                                let client_id = v
                                                    .get("client_id")
                                                    .and_then(|t| t.as_str())
                                                    .unwrap_or("");
                                                let client_secret = v
                                                    .get("client_secret")
                                                    .and_then(|t| t.as_str())
                                                    .unwrap_or("");
                                                let resp_body = if client_id.is_empty()
                                                    || client_secret.is_empty()
                                                {
                                                    serde_json::json!({"error":"invalid credentials"}).to_string().into_bytes()
                                                } else {
                                                    let token =
                                                        format!("mock:{}:control", client_id);
                                                    serde_json::json!({
                                                        "access_token": token,
                                                        "token_type": "Bearer",
                                                        "expires_in": 3600
                                                    })
                                                    .to_string()
                                                    .into_bytes()
                                                };
                                                let _ = {
                                                    // Build token response payload
                                                    let mut req_payload = Vec::new();
                                                    fr::build_tlv(
                                                        fr::TAG_ID,
                                                        pubref.id.as_bytes(),
                                                        &mut req_payload,
                                                    );
                                                    fr::build_tlv(
                                                        fr::TAG_BODY,
                                                        &resp_body,
                                                        &mut req_payload,
                                                    );
                                                    fr::build_tlv(
                                                        fr::TAG_STREAM_END,
                                                        &[],
                                                        &mut req_payload,
                                                    );

                                                    engine_task
                                                        .dispatch(
                                                            reply_route,
                                                            req_payload,
                                                            0,
                                                            route_family,
                                                        )
                                                        .await
                                                };
                                            } else {
                                                let _ = send_err_chan(
                                                    mux_task.clone(),
                                                    parsed.header.channel_id,
                                                    1003,
                                                    "missing reply route",
                                                    None,
                                                )
                                                .await;
                                                ack_sent = true;
                                            }
                                        } else {
                                            let _ = send_err_chan(
                                                mux_task.clone(),
                                                parsed.header.channel_id,
                                                1010,
                                                "bad pub",
                                                None,
                                            )
                                            .await;
                                            ack_sent = true;
                                        }
                                        maybe_ack_and_decrement(
                                            mux_task.clone(),
                                            needs_ack && !ack_sent,
                                            parsed.header.channel_id,
                                            inflight_task.clone(),
                                        )
                                        .await;
                                        return;
                                    }
                                }

                                let tenant_opt = {
                                    let g = auth_task.lock().await;
                                    g.clone()
                                };
                                if tenant_opt.is_none() {
                                    let _ = send_err_chan(
                                        mux_task.clone(),
                                        parsed.header.channel_id,
                                        1001,
                                        "not authenticated",
                                        None,
                                    )
                                    .await;
                                    ack_sent = true;
                                    maybe_ack_and_decrement(
                                        mux_task.clone(),
                                        needs_ack && !ack_sent,
                                        parsed.header.channel_id,
                                        inflight_task.clone(),
                                    )
                                    .await;
                                    return;
                                }

                                if let Ok(pubref) = fr::parse_pub(&parsed) {
                                    // Enforce payload size limit (1 MiB default per SPEC)
                                    const MAX_PAYLOAD: usize = 1_048_576;
                                    let limit = MAX_PAYLOAD;
                                    if pubref.body.len() > limit {
                                        let _ = send_err_chan(
                                            mux_task.clone(),
                                            parsed.header.channel_id,
                                            1009,
                                            "payload too large",
                                            None,
                                        )
                                        .await;
                                        ack_sent = true;
                                        maybe_ack_and_decrement(
                                            mux_task.clone(),
                                            needs_ack && !ack_sent,
                                            parsed.header.channel_id,
                                            inflight_task.clone(),
                                        )
                                        .await;
                                        return;
                                    }

                                    let mut route_str = pubref.route.to_string();
                                    // inbox:// mapping -> rpc/reply/<path>
                                    if route_str.starts_with("inbox://") {
                                        if let Some(rest) = route_str.strip_prefix("inbox://") {
                                            route_str = format!("rpc/reply/{}", rest);
                                        }
                                    }

                                    // Parse route or allow bare dev routes for notice and rpc reply for backward-compat
                                    let parsed_route = route_mod::parse_route(&route_str);
                                    if let Ok(r) = parsed_route {
                                        let tenant = tenant_opt.clone().unwrap();
                                        if !route_mod::realm_matches(&r, &tenant) {
                                            let _ = send_err_chan(
                                                mux_task.clone(),
                                                parsed.header.channel_id,
                                                1003,
                                                "realm mismatch",
                                                None,
                                            )
                                            .await;
                                            ack_sent = true;
                                            maybe_ack_and_decrement(
                                                mux_task.clone(),
                                                needs_ack && !ack_sent,
                                                parsed.header.channel_id,
                                                inflight_task.clone(),
                                            )
                                            .await;
                                            return;
                                        }

                                        // Permission check (baseline permissive under the hood)
                                        if !permissions::has_permission(
                                            &tenant,
                                            &route_str,
                                            Action::Pub,
                                        ) {
                                            let _ = send_err_chan(
                                                mux_task.clone(),
                                                parsed.header.channel_id,
                                                1002,
                                                "forbidden",
                                                None,
                                            )
                                            .await;
                                            ack_sent = true;
                                            maybe_ack_and_decrement(
                                                mux_task.clone(),
                                                needs_ack && !ack_sent,
                                                parsed.header.channel_id,
                                                inflight_task.clone(),
                                            )
                                            .await;
                                            return;
                                        }
                                    } else {
                                        // If no scheme, allow only notice-like routes and rpc reply routes as a dev convenience
                                        if !(route_str.starts_with("ntc/")
                                            || route_str.starts_with("rpc/reply/"))
                                        {
                                            let _ = send_err_chan(
                                                mux_task.clone(),
                                                parsed.header.channel_id,
                                                1003,
                                                "bad route",
                                                None,
                                            )
                                            .await;
                                            ack_sent = true;
                                            maybe_ack_and_decrement(
                                                mux_task.clone(),
                                                needs_ack && !ack_sent,
                                                parsed.header.channel_id,
                                                inflight_task.clone(),
                                            )
                                            .await;
                                            return;
                                        }
                                    }

                                    let reply_to =
                                        fr::find_tlv(parsed.payload, fr::TAG_ROUTE_REPLY)
                                            .map(|b| String::from_utf8_lossy(b).to_string());
                                    let seq = fr::find_tlv(parsed.payload, fr::TAG_SEQ)
                                        .and_then(|b| b.get(..4))
                                        .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]));
                                    let end =
                                        fr::find_tlv(parsed.payload, fr::TAG_STREAM_END).is_some();
                                    let ttl_secs = fr::find_tlv(parsed.payload, fr::TAG_TTL_SECS)
                                        .and_then(|b| {
                                            if b.len() >= 8 {
                                                Some(u64::from_be_bytes([
                                                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                                                ]))
                                            } else {
                                                None
                                            }
                                        });

                                    if route_str.starts_with("stream://") {
                                        // Build stream append payload
                                        let mut req_payload = Vec::new();
                                        if !pubref.id.is_empty() {
                                            fr::build_tlv(
                                                fr::TAG_ID,
                                                pubref.id.as_bytes(),
                                                &mut req_payload,
                                            );
                                        }
                                        fr::build_tlv(fr::TAG_BODY, &pubref.body, &mut req_payload);

                                        match engine_task
                                            .dispatch(
                                                route_str.clone(),
                                                req_payload,
                                                0,
                                                route_family,
                                            )
                                            .await
                                        {
                                            Ok(response) => {
                                                // Parse sequence number from response
                                                if let Some(seq_bytes) =
                                                    fr::find_tlv(&response, fr::TAG_SEQ)
                                                {
                                                    if seq_bytes.len() == 8 {
                                                        let seq_assigned = u64::from_be_bytes([
                                                            seq_bytes[0],
                                                            seq_bytes[1],
                                                            seq_bytes[2],
                                                            seq_bytes[3],
                                                            seq_bytes[4],
                                                            seq_bytes[5],
                                                            seq_bytes[6],
                                                            seq_bytes[7],
                                                        ]);
                                                        let mut p = Vec::new();
                                                        fr::build_tlv(
                                                            fr::TAG_SEQ,
                                                            &seq_assigned.to_be_bytes(),
                                                            &mut p,
                                                        );
                                                        let ack = fr::build_frame(
                                                            fr::FRAME_ACK,
                                                            0,
                                                            parsed.header.channel_id,
                                                            &p,
                                                        );
                                                        mux_task.send_on_channel(ack).await;
                                                        ack_sent = true;
                                                    } else {
                                                        let _ = send_err_chan(
                                                            mux_task.clone(),
                                                            parsed.header.channel_id,
                                                            1010,
                                                            "invalid TAG_SEQ in response",
                                                            None,
                                                        )
                                                        .await;
                                                        ack_sent = true;
                                                    }
                                                } else {
                                                    let _ = send_err_chan(
                                                        mux_task.clone(),
                                                        parsed.header.channel_id,
                                                        1010,
                                                        "missing TAG_SEQ in response",
                                                        None,
                                                    )
                                                    .await;
                                                    ack_sent = true;
                                                }
                                            }
                                            Err(e) => {
                                                let _ = send_err_chan(
                                                    mux_task.clone(),
                                                    parsed.header.channel_id,
                                                    1010,
                                                    &format!("stream append failed: {}", e),
                                                    None,
                                                )
                                                .await;
                                                ack_sent = true;
                                            }
                                        }
                                    } else {
                                        // Build publish payload
                                        let mut req_payload = Vec::new();
                                        fr::build_tlv(
                                            fr::TAG_ID,
                                            pubref.id.as_bytes(),
                                            &mut req_payload,
                                        );
                                        fr::build_tlv(fr::TAG_BODY, &pubref.body, &mut req_payload);

                                        if let Some(reply) = &reply_to {
                                            fr::build_tlv(
                                                fr::TAG_ROUTE_REPLY,
                                                reply.as_bytes(),
                                                &mut req_payload,
                                            );
                                        }
                                        if let Some(s) = seq {
                                            fr::build_tlv(
                                                fr::TAG_SEQ,
                                                &s.to_be_bytes(),
                                                &mut req_payload,
                                            );
                                        }
                                        if end {
                                            fr::build_tlv(
                                                fr::TAG_STREAM_END,
                                                &[],
                                                &mut req_payload,
                                            );
                                        }
                                        if let Some(ttl) = ttl_secs {
                                            fr::build_tlv(
                                                fr::TAG_TTL_SECS,
                                                &ttl.to_be_bytes(),
                                                &mut req_payload,
                                            );
                                        }

                                        match engine_task
                                            .dispatch(
                                                route_str.clone(),
                                                req_payload,
                                                0,
                                                route_family,
                                            )
                                            .await
                                        {
                                            Ok(_response) => {}
                                            Err(e) => {
                                                let _ = send_err_chan(
                                                    mux_task.clone(),
                                                    parsed.header.channel_id,
                                                    1010,
                                                    &format!("publish failed: {}", e),
                                                    None,
                                                )
                                                .await;
                                                ack_sent = true;
                                            }
                                        }
                                    }
                                } else {
                                    let _ = send_err_chan(
                                        mux_task.clone(),
                                        parsed.header.channel_id,
                                        1010,
                                        "bad pub",
                                        None,
                                    )
                                    .await;
                                    ack_sent = true;
                                }
                            }

                            x if x == fr::FRAME_REG => {
                                let tenant_opt = {
                                    let g = auth_task.lock().await;
                                    g.clone()
                                };
                                if tenant_opt.is_none() {
                                    let _ = send_err_chan(
                                        mux_task.clone(),
                                        parsed.header.channel_id,
                                        1001,
                                        "not authenticated",
                                        None,
                                    )
                                    .await;
                                    ack_sent = true;
                                    maybe_ack_and_decrement(
                                        mux_task.clone(),
                                        needs_ack && !ack_sent,
                                        parsed.header.channel_id,
                                        inflight_task.clone(),
                                    )
                                    .await;
                                    return;
                                }

                                match fr::parse_reg(&parsed) {
                                    Ok(reg) => {
                                        let mut route = reg.route.to_string();
                                        if route.starts_with("inbox://") {
                                            if let Some(rest) = route.strip_prefix("inbox://") {
                                                route = format!("rpc/reply/{}", rest);
                                            }
                                        }

                                        // Accept either full scheme route or bare dev notice/rpc reply routes
                                        if let Ok(r) = route_mod::parse_route(&route) {
                                            let tenant = tenant_opt.clone().unwrap();
                                            if !route_mod::realm_matches(&r, &tenant) {
                                                let _ = send_err_chan(
                                                    mux_task.clone(),
                                                    parsed.header.channel_id,
                                                    1003,
                                                    "realm mismatch",
                                                    None,
                                                )
                                                .await;
                                                ack_sent = true;
                                                maybe_ack_and_decrement(
                                                    mux_task.clone(),
                                                    needs_ack && !ack_sent,
                                                    parsed.header.channel_id,
                                                    inflight_task.clone(),
                                                )
                                                .await;
                                                return;
                                            }

                                            // Permission check: subscribe implies read
                                            if !permissions::has_permission(
                                                &tenant,
                                                &route,
                                                Action::Read,
                                            ) {
                                                let _ = send_err_chan(
                                                    mux_task.clone(),
                                                    parsed.header.channel_id,
                                                    1002,
                                                    "forbidden",
                                                    None,
                                                )
                                                .await;
                                                ack_sent = true;
                                                maybe_ack_and_decrement(
                                                    mux_task.clone(),
                                                    needs_ack && !ack_sent,
                                                    parsed.header.channel_id,
                                                    inflight_task.clone(),
                                                )
                                                .await;
                                                return;
                                            }
                                        } else if !(route.starts_with("ntc/")
                                            || route.starts_with("rpc/reply/"))
                                        {
                                            let _ = send_err_chan(
                                                mux_task.clone(),
                                                parsed.header.channel_id,
                                                1003,
                                                "bad route",
                                                None,
                                            )
                                            .await;
                                            ack_sent = true;
                                            maybe_ack_and_decrement(
                                                mux_task.clone(),
                                                needs_ack && !ack_sent,
                                                parsed.header.channel_id,
                                                inflight_task.clone(),
                                            )
                                            .await;
                                            return;
                                        }

                                        // TODO: Subscribe/unsubscribe handled via Notice domain dispatch
                                        // For now, just send ACK
                                        let mut p = Vec::new();
                                        fr::build_tlv(fr::TAG_ROUTE, route.as_bytes(), &mut p);
                                        let ack = fr::build_frame(
                                            fr::FRAME_ACK,
                                            0,
                                            parsed.header.channel_id,
                                            &p,
                                        );
                                        mux_task.send_on_channel(ack).await;
                                        ack_sent = true;
                                    }
                                    Err(_) => {
                                        let _ = send_err_chan(
                                            mux_task.clone(),
                                            parsed.header.channel_id,
                                            1010,
                                            "bad reg",
                                            None,
                                        )
                                        .await;
                                        ack_sent = true;
                                    }
                                }
                            }

                            x if x == fr::FRAME_REQ => {
                                let req_id: Option<Vec<u8>> = None;
                                let route_b = fr::find_tlv(parsed.payload, fr::TAG_ROUTE);
                                let id_b = fr::find_tlv(parsed.payload, fr::TAG_ID);
                                let lease_b = fr::find_tlv(parsed.payload, fr::TAG_LEASE);
                                let token_b = fr::find_tlv(parsed.payload, fr::TAG_DELIVERY_TOKEN);
                                let auth_opt = {
                                    let g = auth_task.lock().await;
                                    g.clone()
                                };
                                if auth_opt.is_none() {
                                    let _ = send_err_chan(
                                        mux_task.clone(),
                                        parsed.header.channel_id,
                                        1001,
                                        "not authenticated",
                                        req_id.as_deref(),
                                    )
                                    .await;
                                    ack_sent = true;
                                    maybe_ack_and_decrement(
                                        mux_task.clone(),
                                        needs_ack && !ack_sent,
                                        parsed.header.channel_id,
                                        inflight_task.clone(),
                                    )
                                    .await;
                                    return;
                                }
                                if route_b.is_none() {
                                    let _ = send_err_chan(
                                        mux_task.clone(),
                                        parsed.header.channel_id,
                                        1003,
                                        "missing route",
                                        req_id.as_deref(),
                                    )
                                    .await;
                                    ack_sent = true;
                                    maybe_ack_and_decrement(
                                        mux_task.clone(),
                                        needs_ack && !ack_sent,
                                        parsed.header.channel_id,
                                        inflight_task.clone(),
                                    )
                                    .await;
                                    return;
                                }
                                let route = String::from_utf8_lossy(route_b.unwrap()).to_string();

                                // Realm enforcement for scheme routes only
                                if let Ok(r) = route_mod::parse_route(&route) {
                                    let tenant = auth_opt.clone().unwrap();
                                    if !route_mod::realm_matches(&r, &tenant) {
                                        let _ = send_err_chan(
                                            mux_task.clone(),
                                            parsed.header.channel_id,
                                            1003,
                                            "realm mismatch",
                                            req_id.as_deref(),
                                        )
                                        .await;
                                        ack_sent = true;
                                        maybe_ack_and_decrement(
                                            mux_task.clone(),
                                            needs_ack && !ack_sent,
                                            parsed.header.channel_id,
                                            inflight_task.clone(),
                                        )
                                        .await;
                                        return;
                                    }

                                    // Permission check: REQ implies consume/lease operations on queue
                                    if !permissions::has_permission(
                                        &tenant,
                                        &route,
                                        Action::Consume,
                                    ) {
                                        let _ = send_err_chan(
                                            mux_task.clone(),
                                            parsed.header.channel_id,
                                            1002,
                                            "forbidden",
                                            req_id.as_deref(),
                                        )
                                        .await;
                                        ack_sent = true;
                                        maybe_ack_and_decrement(
                                            mux_task.clone(),
                                            needs_ack && !ack_sent,
                                            parsed.header.channel_id,
                                            inflight_task.clone(),
                                        )
                                        .await;
                                        return;
                                    }
                                }

                                // Branches:
                                // 1) Extend lease: TAG_ID + TAG_DELIVERY_TOKEN + TAG_LEASE
                                if let (Some(id_raw), Some(tok_raw), Some(lease_raw)) =
                                    (id_b, token_b, lease_b)
                                {
                                    if lease_raw.len() < 4 {
                                        let _ = send_err_chan(
                                            mux_task.clone(),
                                            parsed.header.channel_id,
                                            1005,
                                            "bad lease value",
                                            req_id.as_deref(),
                                        )
                                        .await;
                                        ack_sent = true;
                                        maybe_ack_and_decrement(
                                            mux_task.clone(),
                                            needs_ack && !ack_sent,
                                            parsed.header.channel_id,
                                            inflight_task.clone(),
                                        )
                                        .await;
                                        return;
                                    }
                                    let id = String::from_utf8_lossy(id_raw).to_string();
                                    let token = String::from_utf8_lossy(tok_raw).to_string();
                                    let add_secs = u32::from_be_bytes([
                                        lease_raw[0],
                                        lease_raw[1],
                                        lease_raw[2],
                                        lease_raw[3],
                                    ]);
                                    // Build request payload
                                    let mut req_payload = Vec::new();
                                    fr::build_tlv(fr::TAG_ID, id.as_bytes(), &mut req_payload);
                                    fr::build_tlv(
                                        fr::TAG_DELIVERY_TOKEN,
                                        token.as_bytes(),
                                        &mut req_payload,
                                    );
                                    fr::build_tlv(
                                        fr::TAG_LEASE,
                                        &add_secs.to_be_bytes(),
                                        &mut req_payload,
                                    );

                                    match engine_task
                                        .dispatch(route.clone(), req_payload, 0, route_family)
                                        .await
                                    {
                                        Ok(response) => {
                                            // Parse remaining seconds from response
                                            if let Some(lease_bytes) =
                                                fr::find_tlv(&response, fr::TAG_LEASE)
                                            {
                                                if lease_bytes.len() == 4 {
                                                    let remaining = u32::from_be_bytes([
                                                        lease_bytes[0],
                                                        lease_bytes[1],
                                                        lease_bytes[2],
                                                        lease_bytes[3],
                                                    ]);
                                                    let mut p = Vec::new();
                                                    fr::build_tlv(
                                                        fr::TAG_ID,
                                                        id.as_bytes(),
                                                        &mut p,
                                                    );
                                                    fr::build_tlv(
                                                        fr::TAG_LEASE,
                                                        &remaining.to_be_bytes(),
                                                        &mut p,
                                                    );
                                                    let ack = fr::build_frame(
                                                        fr::FRAME_ACK,
                                                        0,
                                                        parsed.header.channel_id,
                                                        &p,
                                                    );
                                                    mux_task.send_on_channel(ack).await;
                                                    ack_sent = true;
                                                } else {
                                                    let _ = send_err_chan(
                                                        mux_task.clone(),
                                                        parsed.header.channel_id,
                                                        1010,
                                                        "invalid lease value in response",
                                                        req_id.as_deref(),
                                                    )
                                                    .await;
                                                    ack_sent = true;
                                                }
                                            } else {
                                                let _ = send_err_chan(
                                                    mux_task.clone(),
                                                    parsed.header.channel_id,
                                                    1010,
                                                    "missing TAG_LEASE in response",
                                                    req_id.as_deref(),
                                                )
                                                .await;
                                                ack_sent = true;
                                            }
                                        }
                                        Err(e) => {
                                            let _ = send_err_chan(
                                                mux_task.clone(),
                                                parsed.header.channel_id,
                                                1010,
                                                &format!("extend failed: {}", e),
                                                req_id.as_deref(),
                                            )
                                            .await;
                                            ack_sent = true;
                                        }
                                    }
                                }
                                // 2) Lease next: TAG_LEASE only (no TAG_ID)
                                else if id_b.is_none() {
                                    let add_secs = if let Some(lease_raw) = lease_b {
                                        if lease_raw.len() >= 4 {
                                            u32::from_be_bytes([
                                                lease_raw[0],
                                                lease_raw[1],
                                                lease_raw[2],
                                                lease_raw[3],
                                            ])
                                        } else {
                                            0
                                        }
                                    } else {
                                        0
                                    };
                                    // Build request payload for reserve (just TAG_LEASE)
                                    let mut req_payload = Vec::new();
                                    fr::build_tlv(
                                        fr::TAG_LEASE,
                                        &add_secs.to_be_bytes(),
                                        &mut req_payload,
                                    );

                                    match engine_task
                                        .dispatch(route.clone(), req_payload, 0, route_family)
                                        .await
                                    {
                                        Ok(response) => {
                                            // Parse response TLVs
                                            if let (Some(id_bytes), Some(body), Some(token_bytes)) = (
                                                fr::find_tlv(&response, fr::TAG_ID),
                                                fr::find_tlv(&response, fr::TAG_BODY),
                                                fr::find_tlv(&response, fr::TAG_DELIVERY_TOKEN),
                                            ) {
                                                if let (Ok(id), Ok(token)) = (
                                                    std::str::from_utf8(id_bytes),
                                                    std::str::from_utf8(token_bytes),
                                                ) {
                                                    let mut p = Vec::new();
                                                    fr::build_tlv(
                                                        fr::TAG_ROUTE,
                                                        route.as_bytes(),
                                                        &mut p,
                                                    );
                                                    fr::build_tlv(
                                                        fr::TAG_ID,
                                                        id.as_bytes(),
                                                        &mut p,
                                                    );
                                                    fr::build_tlv(fr::TAG_BODY, body, &mut p);
                                                    fr::build_tlv(
                                                        fr::TAG_DELIVERY_TOKEN,
                                                        token.as_bytes(),
                                                        &mut p,
                                                    );
                                                    fr::build_tlv(
                                                        fr::TAG_LEASE,
                                                        &add_secs.to_be_bytes(),
                                                        &mut p,
                                                    );
                                                    let frame = fr::build_frame(
                                                        fr::FRAME_DAT,
                                                        0,
                                                        parsed.header.channel_id,
                                                        &p,
                                                    );
                                                    mux_task.send_on_channel(frame).await;
                                                } else {
                                                    let _ = send_err_chan(
                                                        mux_task.clone(),
                                                        parsed.header.channel_id,
                                                        1010,
                                                        "invalid text encoding in response",
                                                        req_id.as_deref(),
                                                    )
                                                    .await;
                                                    ack_sent = true;
                                                }
                                            } else {
                                                let _ = send_err_chan(
                                                    mux_task.clone(),
                                                    parsed.header.channel_id,
                                                    1010,
                                                    "missing required TLVs in response",
                                                    req_id.as_deref(),
                                                )
                                                .await;
                                                ack_sent = true;
                                            }
                                        }
                                        Err(e) => {
                                            let _ = send_err_chan(
                                                mux_task.clone(),
                                                parsed.header.channel_id,
                                                1010,
                                                &format!("reserve failed: {}", e),
                                                req_id.as_deref(),
                                            )
                                            .await;
                                            ack_sent = true;
                                        }
                                    }
                                }
                                // 3) Complete: TAG_ID + TAG_DELIVERY_TOKEN
                                else if let (Some(id_raw), Some(tok_raw)) = (id_b, token_b) {
                                    let id = String::from_utf8_lossy(id_raw).to_string();
                                    let token = String::from_utf8_lossy(tok_raw).to_string();
                                    // Build request payload
                                    let mut req_payload = Vec::new();
                                    fr::build_tlv(fr::TAG_ID, id.as_bytes(), &mut req_payload);
                                    fr::build_tlv(
                                        fr::TAG_DELIVERY_TOKEN,
                                        token.as_bytes(),
                                        &mut req_payload,
                                    );

                                    match engine_task
                                        .dispatch(route.clone(), req_payload, 0, route_family)
                                        .await
                                    {
                                        Ok(_response) => {
                                            let mut p = Vec::new();
                                            fr::build_tlv(fr::TAG_ROUTE, route.as_bytes(), &mut p);
                                            fr::build_tlv(fr::TAG_ID, id.as_bytes(), &mut p);
                                            let ack = fr::build_frame(
                                                fr::FRAME_ACK,
                                                0,
                                                parsed.header.channel_id,
                                                &p,
                                            );
                                            mux_task.send_on_channel(ack).await;
                                            ack_sent = true;
                                        }
                                        Err(e) => {
                                            let _ = send_err_chan(
                                                mux_task.clone(),
                                                parsed.header.channel_id,
                                                1010,
                                                &format!("consume failed: {}", e),
                                                req_id.as_deref(),
                                            )
                                            .await;
                                            ack_sent = true;
                                        }
                                    }
                                } else {
                                    let _ = send_err_chan(
                                        mux_task.clone(),
                                        parsed.header.channel_id,
                                        1003,
                                        "bad request",
                                        req_id.as_deref(),
                                    )
                                    .await;
                                    ack_sent = true;
                                }
                            }

                            _ => {}
                        }

                        if needs_ack {
                            if let Ok(parsed) = fr::parse_frame(&frame_bytes) {
                                if !ack_sent {
                                    let ack = fr::build_frame(
                                        fr::FRAME_ACK,
                                        0,
                                        parsed.header.channel_id,
                                        &[],
                                    );
                                    mux_task.send_on_channel(ack).await;
                                }

                                // Metrics only: track inflight (best-effort)
                                let mut in_g = inflight_task.lock().await;
                                if *in_g > 0 {
                                    *in_g -= 1;
                                }
                            }

                            // Drop the permit by dropping the OwnedSemaphorePermit
                            drop(_permit_owned);
                        }
                    }
                });
            }
        }

        // Connection dropped: notify all domains to cleanup resources for this channel
        let route_family = {
            let tenant_opt = auth_state.lock().await;
            match tenant_opt.as_ref() {
                Some(tenant) => tenant_to_route_family(tenant),
                None => crate::storage::DEFAULT_RF,
            }
        };
        let _ = engine_clone.cleanup_channel(channel, route_family).await;
    });
}

// helper: send an ERR on a specific channel and optionally echo req_id
async fn send_err_chan(
    mux: Arc<Muxer>,
    channel_id: u32,
    code: u32,
    message: &str,
    _req_id: Option<&[u8]>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut p = Vec::new();
    fr::build_tlv(fr::TAG_ERR_CODE, &code.to_be_bytes(), &mut p);
    fr::build_tlv(fr::TAG_ERR_MSG, message.as_bytes(), &mut p);
    let frame = fr::build_frame(fr::FRAME_ERR, 0, channel_id, &p);
    mux.send_on_channel(frame).await;
    Ok(())
}

// helper: if ACK_REQUIRED and no ack sent, send auto-ack; always decrement inflight
async fn maybe_ack_and_decrement(
    mux: Arc<Muxer>,
    ack_needed: bool,
    channel_id: u32,
    inflight: Arc<tokio::sync::Mutex<usize>>,
) {
    if ack_needed {
        let ack = fr::build_frame(fr::FRAME_ACK, 0, channel_id, &[]);
        mux.send_on_channel(ack).await;
    }
    let mut in_g = inflight.lock().await;
    if *in_g > 0 {
        *in_g -= 1;
    }
}

// Read just the header fields: (frame_type, flags, channel_id)
fn read_header(buf: &[u8]) -> Option<(u8, u8, u32)> {
    if buf.len() < 10 {
        return None;
    }
    let frame_type = buf[4];
    let flags = buf[5];
    let chan = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]);
    Some((frame_type, flags, chan))
}
