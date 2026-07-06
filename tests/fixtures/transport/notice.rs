use super::*;

// ============================================================================
// NOTICE DOMAIN - CONNECTOR IMPLEMENTATIONS
// ============================================================================

pub struct TcpNoticeConnector(TestClient);
pub struct WsNoticeConnector(TestWebSocketClient);

impl HasFixtureClient for TcpNoticeConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsNoticeConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait NoticeConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl NoticeConnector for TcpNoticeConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpNoticeConnector)
    }
}

#[async_trait::async_trait]
impl NoticeConnector for WsNoticeConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsNoticeConnector)
    }
}

/// Build NOTICE PUBLISH frame (`msg_type` 500)
pub fn build_notice_publish(route: &str, _realm: &str, data: &[u8]) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string route][bytes payload]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    // Payload (length-prefixed bytes)
    buf.put_u32(u32_len(data.len()));
    buf.put_slice(data);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(500, &buf);
    builder.build()
}

/// Build NOTICE SUBSCRIBE frame (`msg_type` 501)
pub fn build_notice_subscribe(route_pattern: &str) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string pattern]
    let mut buf = Vec::new();

    // Pattern (length-prefixed string)
    buf.put_u32(u32_len(route_pattern.len()));
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(501, &buf);
    builder.build()
}

/// Build NOTICE UNSUBSCRIBE frame (`msg_type` 502)
pub fn build_notice_unsubscribe(subscription_id: u64) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u64(subscription_id);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(502, &buf);
    builder.build()
}

/// Parse NOTICE response
pub fn parse_notice_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][...response data...]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        // Return msg_type (as u8), status, and data portion (skipping status byte)
        let data = if payload.len() > 1 {
            payload[1..].to_vec()
        } else {
            Vec::new()
        };
        return (msg_type_to_u8(msg_type), status, data);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

pub fn parse_notice_subscription_id(data: &[u8]) -> Result<Option<u64>, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut decoder = PayloadDecoder::new(data);
    let subscription_id = decoder.get_optional_u64()?;
    if !decoder.is_complete() {
        return Err("Trailing data in notice subscription response".to_string());
    }

    Ok(subscription_id)
}

pub struct NoticeDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub route: String,
    pub body: Vec<u8>,
}

pub fn parse_notice_delivery(frame: &[u8]) -> Result<NoticeDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing notice delivery frame".to_string())?;
    if msg_type != 504 {
        return Err(format!("Unexpected notice delivery msg_type: {msg_type}"));
    }

    let mut dec = PayloadDecoder::new(&payload);
    let subscription_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let body = dec.get_bytes()?.to_vec();
    if !dec.is_complete() {
        return Err("Trailing data in notice delivery".to_string());
    }

    Ok(NoticeDelivery {
        msg_type,
        subscription_id,
        route,
        body,
    })
}
