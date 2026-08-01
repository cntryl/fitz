use super::*;

// ============================================================================
// LEASE DOMAIN - CONNECTOR IMPLEMENTATIONS
// ============================================================================

pub struct TcpLeaseConnector(TestClient);
pub struct WsLeaseConnector(TestWebSocketClient);

impl HasFixtureClient for TcpLeaseConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsLeaseConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait LeaseConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl LeaseConnector for TcpLeaseConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpLeaseConnector)
    }
}

#[async_trait::async_trait]
impl LeaseConnector for WsLeaseConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsLeaseConnector)
    }
}

/// Build LEASE ACQUIRE frame (`msg_type` 400)
pub fn build_lease_acquire_immediate(route: &str, owner_id: &str, ttl_secs: i32) -> Vec<u8> {
    // Wire format: [string route][string owner_id][u64 ttl_secs][u32 wait_seconds (optional)]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    // Owner ID (length-prefixed string)
    buf.put_u32(u32_len(owner_id.len()));
    buf.put_slice(owner_id.as_bytes());

    // TTL seconds (u64)
    buf.put_u64(u64_from_i32(ttl_secs));

    // Wait seconds (u32, 0 for immediate)
    buf.put_u32(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(400, &buf);
    builder.build()
}

/// Build LEASE ACQUIRE frame (`msg_type` 400) with waiting.
pub fn build_lease_acquire_with_wait(
    route: &str,
    owner_id: &str,
    ttl_secs: i32,
    wait_seconds: u32,
) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    buf.put_u32(u32_len(owner_id.len()));
    buf.put_slice(owner_id.as_bytes());

    buf.put_u64(u64_from_i32(ttl_secs));
    buf.put_u32(wait_seconds);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(400, &buf);
    builder.build()
}

/// Build LEASE RENEW frame (`msg_type` 401)
pub fn build_lease_renew(route: &str, owner_id: &str, token: u64, ttl_secs: i32) -> Vec<u8> {
    // Wire format: [string route][string owner_id][u64 fencing_token][u64 ttl_secs]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    // Owner ID (length-prefixed string)
    buf.put_u32(u32_len(owner_id.len()));
    buf.put_slice(owner_id.as_bytes());

    // Fencing token (u64)
    buf.put_u64(token);

    // TTL seconds (u64)
    buf.put_u64(u64_from_i32(ttl_secs));

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(401, &buf);
    builder.build()
}

/// Build LEASE RELEASE frame (`msg_type` 402)
pub fn build_lease_release(route: &str, owner_id: &str, token: u64) -> Vec<u8> {
    // Wire format: [string route][string owner_id][u64 fencing_token]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    // Owner ID (length-prefixed string)
    buf.put_u32(u32_len(owner_id.len()));
    buf.put_slice(owner_id.as_bytes());

    // Fencing token (u64)
    buf.put_u64(token);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(402, &buf);
    builder.build()
}

/// Build LEASE QUERY frame (`msg_type` 403)
pub fn build_lease_query(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(403, &buf);
    builder.build()
}

/// Build LEASE SUBSCRIBE frame (`msg_type` 407)
pub fn build_lease_subscribe(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(407, &buf);
    builder.build()
}

/// Build LEASE UNSUBSCRIBE frame (`msg_type` 408)
pub fn build_lease_unsubscribe(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(408, &buf);
    builder.build()
}

pub fn extract_lease_subscription_id(data: &[u8]) -> Result<u64, String> {
    if data.len() < 9 {
        return Err("Lease subscribe response too short".to_string());
    }

    Ok(u64::from_be_bytes(data[1..9].try_into().map_err(|_| {
        "Lease subscribe response missing subscription id".to_string()
    })?))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseWatchDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub route: String,
    pub payload: Vec<u8>,
}

pub fn parse_lease_watch_delivery(frame: &[u8]) -> Result<LeaseWatchDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing lease watch delivery frame".to_string())?;
    if msg_type != 409 {
        return Err(format!(
            "Unexpected lease watch delivery msg_type: {msg_type}"
        ));
    }

    let mut decoder = PayloadDecoder::new(&payload);
    let subscription_id = decoder.get_u64()?;
    let route = decoder.get_string()?;
    let payload = decoder.get_bytes()?.to_vec();
    if !decoder.is_complete() {
        return Err("Trailing data in lease watch delivery".to_string());
    }

    Ok(LeaseWatchDelivery {
        msg_type,
        subscription_id,
        route,
        payload,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseStatusPayload {
    pub has_holder: bool,
    pub owner_id: Option<String>,
    pub expires_in_secs: Option<u64>,
    pub pending_waiters: u32,
}

/// Parse LEASE response: (`msg_type: u8`, `status: u8`, `data: Vec<u8>`)
pub fn parse_lease_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    // Server sends single TLV record: [`msg_type`][`len`][`payload`]
    // Payload format: [`u8` `status`][optional `u64` token]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        // Return `msg_type` (as `u8`), `status`, and full payload for further parsing
        return (msg_type_to_u8(msg_type), status, payload);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

/// Parse lease token from `ACQUIRE` success response data (`CLIENT_SPEC`).
/// Wire format: [`u8 status=0`][`u8 response_type` (`0=Acquired`, `1=AlreadyHeld`, `2=Queued`, `3=AlreadyQueued`)][`u64` BE `fencing_token`]
pub fn parse_lease_token_response(data: &[u8]) -> Result<u64, String> {
    if data.len() < 10 {
        return Err("Token data too short".to_string());
    }

    let status = data[0];
    if status != 0 {
        return Err("Lease operation failed".to_string());
    }

    // Bytes 2-9: `fencing_token` (`u64` big-endian)
    let bytes = [
        data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],
    ];
    Ok(u64::from_be_bytes(bytes))
}

pub fn parse_lease_acquire_response_type(data: &[u8]) -> Result<u8, String> {
    if data.len() < 2 {
        return Err("Acquire response too short".to_string());
    }

    if data[0] != 0 {
        return Err("Lease operation failed".to_string());
    }

    Ok(data[1])
}

pub fn parse_lease_error_message(data: &[u8]) -> Result<String, String> {
    fitz::protocol::error_codes::decode_error_body(data).map(|(_, message)| message)
}

pub fn parse_lease_status_payload(data: &[u8]) -> Result<LeaseStatusPayload, String> {
    let mut decoder = fitz::protocol::payload_codec::PayloadDecoder::new(data);
    let status = decoder.get_u8()?;
    if status != 0 {
        return Err("Lease operation failed".to_string());
    }

    let has_holder = decoder.get_u8()? != 0;
    if !has_holder {
        let pending_waiters = decoder.get_u32()?;
        return Ok(LeaseStatusPayload {
            has_holder: false,
            owner_id: None,
            expires_in_secs: None,
            pending_waiters,
        });
    }

    let owner_id = decoder.get_string()?;
    let expires_in_secs = decoder.get_u64()?;
    let pending_waiters = decoder.get_u32()?;
    Ok(LeaseStatusPayload {
        has_holder: true,
        owner_id: Some(owner_id),
        expires_in_secs: Some(expires_in_secs),
        pending_waiters,
    })
}
