use crate::core::domain::{Domain, DomainContext, DomainResponse};
use crate::core::parsing::tlv;
use crate::core::lease::service::LeaseService;
use crate::core::lease::types::LeaseOperation;
use crate::protocol::frame::build_tlv;
use crate::protocol::tags::*;
use std::sync::Arc;

/// Lease domain handler - routes all lease:// operations
///
/// Architecture:
/// - Instance-owned LeaseService for per-domain isolation
/// - Shared Arc<LeaseService> allows multi-tenant access via route_family
/// - Internal DashMap concurrency in LeaseService (no RwLock wrapper needed)
pub struct LeaseDomain {
    service: Arc<LeaseService>,
}

impl LeaseDomain {
    pub fn new() -> Self {
        Self {
            service: LeaseService::new(),
        }
    }

    /// Get the shared lease service for use by other domains (e.g., control)
    pub fn get_service(&self) -> Arc<LeaseService> {
        Arc::clone(&self.service)
    }
}

impl Default for LeaseDomain {
    fn default() -> Self {
        Self::new()
    }
}

impl Domain for LeaseDomain {
    fn handle<'a>(
        &'a self,
        _request: DomainContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(self.process(_request))
    }
}

impl LeaseDomain {
    async fn process(&self, request: DomainContext) -> DomainResponse {
        let svc = &self.service;
        let rf = request.route_family;

        // Parse operation from route
        let operation = match LeaseOperation::from_route(&request.route) {
            Ok(op) => op,
            Err(err) => {
                return DomainResponse::Error(err);
            }
        };

        let key = request.route.raw;
        let payload = request.payload;

        // Route to appropriate handler based on operation and validate required TLV tags
        match operation {
            LeaseOperation::Acquire => {
                // Acquire requires TAG_LEASE (TTL)
                match tlv::parse_u32(&payload, TAG_LEASE) {
                    Some(ttl) => self.handle_acquire(svc, rf, key, ttl).await,
                    None => DomainResponse::Error("acquire requires TAG_LEASE (TTL)".to_string()),
                }
            }
            LeaseOperation::Renew => {
                // Renew requires TAG_ID, TAG_DELIVERY_TOKEN, TAG_LEASE (additional time)
                let id = tlv::parse_string(&payload, TAG_ID);
                let token = tlv::parse_string(&payload, TAG_DELIVERY_TOKEN);
                let add = tlv::parse_u32(&payload, TAG_LEASE);

                match (id, token, add) {
                    (Some(id), Some(token), Some(add)) => {
                        self.handle_renew(svc, rf, key, id, token, add).await
                    }
                    _ => DomainResponse::Error(
                        "renew requires TAG_ID, TAG_DELIVERY_TOKEN, and TAG_LEASE".to_string(),
                    ),
                }
            }
            LeaseOperation::Release => {
                // Release requires TAG_ID and TAG_DELIVERY_TOKEN
                let id = tlv::parse_string(&payload, TAG_ID);
                let token = tlv::parse_string(&payload, TAG_DELIVERY_TOKEN);

                match (id, token) {
                    (Some(id), Some(token)) => self.handle_release(svc, rf, key, id, token).await,
                    _ => DomainResponse::Error(
                        "release requires TAG_ID and TAG_DELIVERY_TOKEN".to_string(),
                    ),
                }
            }
        }
    }

    async fn handle_acquire(
        &self,
        svc: &Arc<LeaseService>,
        rf: crate::storage::RouteFamilyId,
        key: String,
        ttl: u32,
    ) -> DomainResponse {
        match svc.acquire(rf, &key, ttl).await {
            Ok(grant) => {
                // pre-allocate output buffer to avoid multiple re-allocations
                let est = grant.id.len()
                    + grant.token.len()
                    + grant.body.as_ref().map(|b| b.len()).unwrap_or(0)
                    + 16;
                let mut out = crate::protocol::frame::take_buf();
                out.clear(); // Ensure buffer is empty before use
                out.reserve(est);
                build_tlv(TAG_ID, grant.id.as_bytes(), &mut out);
                if let Some(body) = &grant.body {
                    build_tlv(TAG_BODY, body, &mut out);
                }
                build_tlv(TAG_DELIVERY_TOKEN, grant.token.as_bytes(), &mut out);
                build_tlv(TAG_LEASE, &grant.ttl_secs.to_be_bytes(), &mut out);
                DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(out))
            }
            Err(e) => DomainResponse::Error(e),
        }
    }

    async fn handle_renew(
        &self,
        svc: &Arc<LeaseService>,
        rf: crate::storage::RouteFamilyId,
        key: String,
        id: &str,
        token: &str,
        add: u32,
    ) -> DomainResponse {
        match svc.renew(rf, &key, id, token, add).await {
            Ok(remaining) => {
                // small response; reserve a few bytes to avoid tiny reallocs
                let mut out = crate::protocol::frame::take_buf();
                out.clear(); // Ensure buffer is empty before use
                out.reserve(8);
                build_tlv(TAG_LEASE, &remaining.to_be_bytes(), &mut out);
                DomainResponse::Frame(crate::protocol::frame::PooledFrame::from_vec(out))
            }
            Err(e) => DomainResponse::Error(e),
        }
    }

    async fn handle_release(
        &self,
        svc: &Arc<LeaseService>,
        rf: crate::storage::RouteFamilyId,
        key: String,
        id: &str,
        token: &str,
    ) -> DomainResponse {
        match svc.surrender(rf, &key, id, token).await {
            Ok(()) => DomainResponse::Ok,
            Err(e) => DomainResponse::Error(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::{DomainContext, DomainResponse};
    use crate::protocol::frame::{build_tlv, find_tlv};
    use crate::protocol::route::parse_route;

    // Helper to build a DomainContext for a given raw route and payload
    fn make_request(raw: &str, payload: Vec<u8>) -> DomainContext {
        // allocate the raw string once and reuse
        let raw_s = raw.to_string();
        let route = parse_route(&raw_s).unwrap();
        DomainContext {
            route,
            route_str: raw_s,
            payload,
            channel_id: 1,
            route_family: 0, // tests use default route family
            sender: None,
        }
    }

    #[test]
    fn should_parse_acquire_operation_from_route() {
        // Arrange
        let route = parse_route("lease://realm1/area1/resource1/acquire").unwrap();

        // Act
        let op = LeaseOperation::from_route(&route);

        // Assert
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), LeaseOperation::Acquire);
    }

    #[test]
    fn should_parse_renew_operation_from_route() {
        // Arrange
        let route = parse_route("lease://realm1/area1/resource1/renew").unwrap();

        // Act
        let op = LeaseOperation::from_route(&route);

        // Assert
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), LeaseOperation::Renew);
    }

    #[test]
    fn should_parse_release_operation_from_route() {
        // Arrange
        let route = parse_route("lease://realm1/area1/resource1/release").unwrap();

        // Act
        let op = LeaseOperation::from_route(&route);

        // Assert
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), LeaseOperation::Release);
    }

    #[test]
    fn should_default_to_acquire_when_no_operation_specified() {
        // Arrange
        let route = parse_route("lease://realm1/area1/resource1").unwrap();

        // Act
        let op = LeaseOperation::from_route(&route);

        // Assert
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), LeaseOperation::Acquire);
    }

    #[test]
    fn should_return_error_for_unknown_operation() {
        // Arrange
        let mut route = parse_route("lease://realm1/area1/resource1").unwrap();
        route.operation = Some("invalid".to_string());

        // Act
        let op = LeaseOperation::from_route(&route);

        // Assert
        assert!(op.is_err());
        assert!(op.unwrap_err().contains("Unknown lease operation"));
    }

    #[tokio::test]
    async fn should_build_tlv_response_for_acquire() {
        // Arrange
        let domain = LeaseDomain::new();
        let mut payload = Vec::new();
        build_tlv(TAG_LEASE, &2u32.to_be_bytes(), &mut payload);
        let req = make_request("lease://realm1/area1/test/acquire", payload);

        // Act
        let resp = domain.handle(req).await;

        // Assert
        match resp {
            DomainResponse::Frame(frame) => {
                let id = find_tlv(frame.as_ref(), TAG_ID);
                let token = find_tlv(frame.as_ref(), TAG_DELIVERY_TOKEN);
                let lease = find_tlv(frame.as_ref(), TAG_LEASE);
                assert!(id.is_some(), "response should contain TAG_ID");
                assert!(
                    token.is_some(),
                    "response should contain TAG_DELIVERY_TOKEN"
                );
                assert!(lease.is_some(), "response should contain TAG_LEASE");
            }
            _ => panic!("expected Frame response for acquire"),
        }
    }

    #[tokio::test]
    async fn should_return_error_when_missing_required_tlv_for_acquire() {
        // Arrange
        let domain = LeaseDomain::new();
        let payload = Vec::new(); // missing TAG_LEASE
        let req = make_request("lease://realm1/area1/test/acquire", payload);

        // Act
        let resp = domain.handle(req).await;

        // Assert
        match resp {
            DomainResponse::Error(msg) => {
                assert!(msg.contains("TAG_LEASE"));
            }
            _ => panic!("expected Error response when missing required TLV"),
        }
    }

    #[tokio::test]
    async fn should_return_error_for_unknown_route_operation() {
        // Arrange
        let domain = LeaseDomain::new();
        let mut payload = Vec::new();
        build_tlv(TAG_LEASE, &2u32.to_be_bytes(), &mut payload);
        let req = make_request("lease://realm1/area1/test/invalid", payload);

        // Act
        let resp = domain.handle(req).await;

        // Assert
        match resp {
            DomainResponse::Error(msg) => {
                assert!(msg.contains("Unknown lease operation"));
            }
            _ => panic!("expected Error response for unknown operation"),
        }
    }
}
