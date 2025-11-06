// Lease domain handler - routes all lease:// operations

use crate::core::domain::{Domain, DomainRequest, DomainResponse};
use crate::core::lease::service::LeaseService;
use crate::protocol::frame::{build_tlv, find_tlv};
use crate::protocol::tags::*;
use std::sync::Arc;

pub struct LeaseDomain;

impl LeaseDomain {
    pub fn new() -> Self {
        // instantiate service (spawn expiration task)
        let _svc = LeaseService::new();
        Self
    }

    // Helper: static service instance
    fn svc() -> &'static Arc<LeaseService> {
        use once_cell::sync::OnceCell;
        static SVC: OnceCell<Arc<LeaseService>> = OnceCell::new();
        SVC.get_or_init(LeaseService::new)
    }

    // Helper: parse u32 TLV
    fn parse_u32(payload: &[u8], tag: u8) -> Option<u32> {
        find_tlv(payload, tag).and_then(|b| {
            if b.len() == 4 {
                Some(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
            } else {
                None
            }
        })
    }

    // Helper: parse string TLV
    fn parse_str(payload: &[u8], tag: u8) -> Option<String> {
        find_tlv(payload, tag).and_then(|b| std::str::from_utf8(b).ok().map(|s| s.to_string()))
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
        _request: DomainRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = DomainResponse> + Send + 'a>> {
        Box::pin(self.process(_request))
    }

    fn schemes(&self) -> &[&str] {
        &["lease"]
    }
}

impl LeaseDomain {
    async fn process(&self, request: DomainRequest) -> DomainResponse {
        let svc = LeaseDomain::svc();

        let key = request.route.raw;
        let payload = request.payload;

        match LeaseDomain::detect_op(&payload) {
            Operation::Acquire(ttl) => self.handle_acquire(svc, key, ttl).await,
            Operation::Extend(id, token, add) => self.handle_extend(svc, key, id, token, add).await,
            Operation::Release(id, token) => self.handle_release(svc, key, id, token).await,
            Operation::Peek => self.handle_peek(svc, key).await,
            Operation::Unsupported => DomainResponse::Error("unsupported_operation".to_string()),
        }
    }

    fn detect_op(payload: &[u8]) -> Operation {
        let tlv_lease = find_tlv(payload, TAG_LEASE);
        let tlv_id = find_tlv(payload, TAG_ID);
        let tlv_token = find_tlv(payload, TAG_DELIVERY_TOKEN);

        if payload.is_empty() {
            return Operation::Peek;
        }

        if tlv_lease.is_some() && tlv_id.is_none() && tlv_token.is_none() {
            if let Some(ttl) = LeaseDomain::parse_u32(payload, TAG_LEASE) {
                return Operation::Acquire(ttl);
            }
            return Operation::Unsupported;
        }

        if tlv_id.is_some() && tlv_token.is_some() && tlv_lease.is_some() {
            if let (Some(id), Some(token), Some(add)) = (
                LeaseDomain::parse_str(payload, TAG_ID),
                LeaseDomain::parse_str(payload, TAG_DELIVERY_TOKEN),
                LeaseDomain::parse_u32(payload, TAG_LEASE),
            ) {
                return Operation::Extend(id, token, add);
            }
            return Operation::Unsupported;
        }

        if tlv_id.is_some() && tlv_token.is_some() && tlv_lease.is_none() {
            if let (Some(id), Some(token)) = (
                LeaseDomain::parse_str(payload, TAG_ID),
                LeaseDomain::parse_str(payload, TAG_DELIVERY_TOKEN),
            ) {
                return Operation::Release(id, token);
            }
            return Operation::Unsupported;
        }

        Operation::Unsupported
    }

    async fn handle_acquire(
        &self,
        svc: &Arc<LeaseService>,
        key: String,
        ttl: u32,
    ) -> DomainResponse {
        match svc.acquire(&key, ttl).await {
            Ok(grant) => {
                let mut out = Vec::new();
                build_tlv(TAG_ID, grant.id.as_bytes(), &mut out);
                if let Some(body) = &grant.body {
                    build_tlv(TAG_BODY, body, &mut out);
                }
                build_tlv(TAG_DELIVERY_TOKEN, grant.token.as_bytes(), &mut out);
                build_tlv(TAG_LEASE, &grant.ttl_secs.to_be_bytes(), &mut out);
                DomainResponse::Frame(out)
            }
            Err(e) => DomainResponse::Error(e),
        }
    }

    async fn handle_extend(
        &self,
        svc: &Arc<LeaseService>,
        key: String,
        id: String,
        token: String,
        add: u32,
    ) -> DomainResponse {
        match svc.extend(&key, &id, &token, add).await {
            Ok(remaining) => {
                let mut out = Vec::new();
                build_tlv(TAG_LEASE, &remaining.to_be_bytes(), &mut out);
                DomainResponse::Frame(out)
            }
            Err(e) => DomainResponse::Error(e),
        }
    }

    async fn handle_release(
        &self,
        svc: &Arc<LeaseService>,
        key: String,
        id: String,
        token: String,
    ) -> DomainResponse {
        match svc.release(&key, &id, &token).await {
            Ok(()) => DomainResponse::Ok,
            Err(e) => DomainResponse::Error(e),
        }
    }

    async fn handle_peek(&self, svc: &Arc<LeaseService>, key: String) -> DomainResponse {
        match svc.peek(&key).await {
            Some((id, body)) => {
                let mut out = Vec::new();
                build_tlv(TAG_ID, id.as_bytes(), &mut out);
                if let Some(b) = body {
                    build_tlv(TAG_BODY, &b, &mut out);
                }
                DomainResponse::Frame(out)
            }
            None => DomainResponse::Frame(Vec::new()),
        }
    }
}

enum Operation {
    Acquire(u32),
    Extend(String, String, u32),
    Release(String, String),
    Peek,
    Unsupported,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::{DomainRequest, DomainResponse};
    use crate::protocol::frame::{build_tlv, find_tlv};
    use crate::protocol::route::{Route, Scheme};

    // Helper to build a DomainRequest for a given raw route and payload
    fn make_request(raw: &str, payload: Vec<u8>) -> DomainRequest {
        DomainRequest {
            route: Route {
                scheme: Scheme::Lease,
                realm: None,
                area: None,
                resource: None,
                operation: None,
                raw: raw.to_string(),
            },
            route_str: raw.to_string(),
            payload,
            channel_id: 1,
        }
    }

    #[test]
    fn should_detect_acquire_operation_from_tlv() {
        // Arrange
        let mut payload = Vec::new();
        build_tlv(TAG_LEASE, &5u32.to_be_bytes(), &mut payload);

        // Act
        let op = LeaseDomain::detect_op(&payload);

        // Assert
        match op {
            Operation::Acquire(ttl) => assert_eq!(ttl, 5),
            _ => panic!("expected Acquire operation"),
        }
    }

    #[test]
    fn should_detect_extend_operation_from_tlv() {
        // Arrange
        let mut payload = Vec::new();
        build_tlv(TAG_ID, b"lease-id-123", &mut payload);
        build_tlv(TAG_DELIVERY_TOKEN, b"token-abc", &mut payload);
        build_tlv(TAG_LEASE, &10u32.to_be_bytes(), &mut payload);

        // Act
        let op = LeaseDomain::detect_op(&payload);

        // Assert
        match op {
            Operation::Extend(id, token, add) => {
                assert_eq!(id, "lease-id-123");
                assert_eq!(token, "token-abc");
                assert_eq!(add, 10);
            }
            _ => panic!("expected Extend operation"),
        }
    }

    #[test]
    fn should_detect_release_operation_from_tlv() {
        // Arrange
        let mut payload = Vec::new();
        build_tlv(TAG_ID, b"lease-id-456", &mut payload);
        build_tlv(TAG_DELIVERY_TOKEN, b"token-xyz", &mut payload);

        // Act
        let op = LeaseDomain::detect_op(&payload);

        // Assert
        match op {
            Operation::Release(id, token) => {
                assert_eq!(id, "lease-id-456");
                assert_eq!(token, "token-xyz");
            }
            _ => panic!("expected Release operation"),
        }
    }

    #[test]
    fn should_detect_peek_operation_from_empty_payload() {
        // Arrange
        let payload = Vec::new();

        // Act
        let op = LeaseDomain::detect_op(&payload);

        // Assert
        match op {
            Operation::Peek => {}
            _ => panic!("expected Peek operation"),
        }
    }

    #[test]
    fn should_detect_unsupported_operation_when_id_without_token() {
        // Arrange
        let mut payload = Vec::new();
        build_tlv(TAG_ID, b"some-id", &mut payload);

        // Act
        let op = LeaseDomain::detect_op(&payload);

        // Assert
        match op {
            Operation::Unsupported => {}
            _ => panic!("expected Unsupported operation"),
        }
    }

    #[tokio::test]
    async fn should_build_tlv_response_for_acquire() {
        // Arrange
        let domain = LeaseDomain::new();
        let mut payload = Vec::new();
        build_tlv(TAG_LEASE, &2u32.to_be_bytes(), &mut payload);
        let req = make_request("lease://test", payload);

        // Act
        let resp = domain.handle(req).await;

        // Assert
        match resp {
            DomainResponse::Frame(frame) => {
                let id = find_tlv(&frame, TAG_ID);
                let token = find_tlv(&frame, TAG_DELIVERY_TOKEN);
                let lease = find_tlv(&frame, TAG_LEASE);
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
    async fn should_build_tlv_response_for_peek_when_lease_exists() {
        // Arrange
        let domain = LeaseDomain::new();
        // first acquire a lease
        let mut acq_payload = Vec::new();
        build_tlv(TAG_LEASE, &2u32.to_be_bytes(), &mut acq_payload);
        let _acq_resp = domain
            .handle(make_request("lease://peek-test", acq_payload))
            .await;

        // Act - peek with empty payload
        let peek_req = make_request("lease://peek-test", Vec::new());
        let resp = domain.handle(peek_req).await;

        // Assert - should return frame with TAG_ID
        match resp {
            DomainResponse::Frame(frame) => {
                let id = find_tlv(&frame, TAG_ID);
                assert!(
                    id.is_some(),
                    "peek response should contain TAG_ID when lease exists"
                );
            }
            _ => panic!("expected Frame response for peek"),
        }
    }

    #[tokio::test]
    async fn should_return_empty_frame_for_peek_when_no_lease() {
        // Arrange
        let domain = LeaseDomain::new();
        let req = make_request("lease://no-lease", Vec::new());

        // Act
        let resp = domain.handle(req).await;

        // Assert
        match resp {
            DomainResponse::Frame(frame) => {
                assert!(
                    frame.is_empty(),
                    "peek should return empty frame when no lease exists"
                );
            }
            _ => panic!("expected Frame response for peek"),
        }
    }

    #[tokio::test]
    async fn should_return_error_for_unsupported_operation() {
        // Arrange
        let domain = LeaseDomain::new();
        let mut payload = Vec::new();
        build_tlv(TAG_ID, b"malformed", &mut payload); // incomplete operation
        let req = make_request("lease://test", payload);

        // Act
        let resp = domain.handle(req).await;

        // Assert
        match resp {
            DomainResponse::Error(msg) => {
                assert_eq!(msg, "unsupported_operation");
            }
            _ => panic!("expected Error response for unsupported operation"),
        }
    }
}
