use super::*;

impl RuntimeIngress {
    pub(super) fn cached_session_inbox_route(
        &self,
        session_id: u64,
    ) -> crate::runtime::routing::Route {
        self.session_inbox_routes
            .get(&session_id)
            .map(|entry| entry.value().clone())
            .unwrap_or_else(|| {
                crate::runtime::routing::Route::new(format!("inbox://session/{session_id}"))
            })
    }

    pub(super) fn domain_dispatch_for_msg_type(
        msg_type: crate::protocol::tlv::MessageType,
    ) -> Result<Option<DomainAuthorizationSpec>, &'static str> {
        use crate::auth::Access;

        let mt = msg_type.as_u16();

        match mt {
            100 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Kv,
                policy: AuthorizationPolicy::KvBeginModeScoped,
            })),
            101..=108 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Kv,
                policy: AuthorizationPolicy::SessionOwned,
            })),
            109 | 110 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Kv,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            111 => Err("invalid message type: 111 is server-to-client only"),
            200..=204 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Queue,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            207 | 208 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Queue,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            209 => Err("invalid message type: 209 is server-to-client only"),
            205 | 206 | 210..=299 => Err("invalid message type: unsupported queue operation"),
            300 | 301 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Rpc,
                policy: AuthorizationPolicy::RouteScoped(Access::All),
            })),
            302 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Rpc,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            303 | 304 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Rpc,
                policy: AuthorizationPolicy::SessionOwned,
            })),
            305..=399 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Rpc,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            400..=402 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Lease,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            403 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Lease,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            407 | 408 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Lease,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            409 => Err("invalid message type: 409 is server-to-client only"),
            404..=406 | 410..=499 => Err("invalid message type: unsupported lease operation"),
            500 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Notice,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            501 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Notice,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            502 | 503 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Notice,
                policy: AuthorizationPolicy::SessionOwned,
            })),
            504 => Err("invalid message type: 504 is server-to-client only"),
            505..=599 => Err("invalid message type: 505-599 are unsupported notice operations"),
            600 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Stream,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            601..=603 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Stream,
                policy: AuthorizationPolicy::SessionOwned,
            })),
            604..=608 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Stream,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            609 => Err("invalid message type: 609 is server-to-client only"),
            700 | 701 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Schedule,
                policy: AuthorizationPolicy::RouteScoped(Access::Write),
            })),
            706 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Schedule,
                policy: AuthorizationPolicy::MultiRouteScoped(Access::Write),
            })),
            702 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Schedule,
                policy: AuthorizationPolicy::WildcardScoped(Access::Read),
            })),
            703 | 704 => Ok(Some(DomainAuthorizationSpec {
                domain: DispatchDomain::Schedule,
                policy: AuthorizationPolicy::RouteScoped(Access::Read),
            })),
            705 => Err("invalid message type: 705 is server-to-client only"),
            _ => Ok(None),
        }
    }

    pub(super) fn resolve_authorization_targets<'a>(
        domain: DispatchDomain,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &'a [u8],
        policy: AuthorizationPolicy,
    ) -> Result<(AuthorizationTargets<'a>, crate::auth::Access), String> {
        match policy {
            AuthorizationPolicy::SessionOwned => Ok((
                AuthorizationTargets::SessionOwned,
                crate::auth::Access::Read,
            )),
            AuthorizationPolicy::WildcardScoped(access) => Ok((
                AuthorizationTargets::Single(Cow::Borrowed(domain.wildcard_route())),
                access,
            )),
            AuthorizationPolicy::KvBeginModeScoped => {
                let access = Self::kv_begin_access(payload)?;
                let route = Self::derive_auth_route_for_frame(domain, msg_type, payload)?
                    .ok_or_else(|| "KV BEGIN authorization route missing".to_string())?;
                Ok((AuthorizationTargets::Single(route), access))
            }
            AuthorizationPolicy::MultiRouteScoped(access) => {
                if domain != DispatchDomain::Schedule || msg_type.as_u16() != 706 {
                    return Err(
                        "multi-route authorization is only supported for schedule batch create"
                            .to_string(),
                    );
                }

                let routes = crate::protocol::schedule_codec::extract_batch_auth_routes(payload)?
                    .into_iter()
                    .map(|route| canonicalize_dispatch_route_str(domain, route))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((AuthorizationTargets::Multiple(routes), access))
            }
            AuthorizationPolicy::RouteScoped(access) => {
                let target = Self::derive_auth_route_for_frame(domain, msg_type, payload)?
                    .map(AuthorizationTargets::Single)
                    .ok_or_else(|| {
                        format!(
                            "{} route-scoped authorization route missing",
                            domain.as_str()
                        )
                    })?;
                Ok((target, access))
            }
        }
    }

    pub(super) fn kv_begin_access(payload: &[u8]) -> Result<crate::auth::Access, String> {
        if payload.len() < 6 {
            return Err("BEGIN payload too short".to_string());
        }

        let route_len =
            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        let mode_offset = 4 + route_len;

        if mode_offset > payload.len() {
            return Err("BEGIN route overflow".to_string());
        }

        if mode_offset >= payload.len() {
            return Err("BEGIN mode byte missing".to_string());
        }

        let access = match payload[mode_offset] {
            0 => crate::auth::Access::Read,
            1 => crate::auth::Access::Write,
            _ => return Err("Invalid transaction mode".to_string()),
        };

        let durability_offset = mode_offset + 1;
        if durability_offset >= payload.len() {
            return Err("BEGIN durability byte missing".to_string());
        }

        match payload[durability_offset] {
            0 | 1 => Ok(access),
            value => Err(format!("Invalid durability mode: {}", value)),
        }
    }
}
