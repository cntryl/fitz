use super::*;

// ============================================================================
// SCHEDULE DOMAIN - CONNECTOR TRAIT
// ============================================================================

pub struct TcpScheduleConnector(TestClient);
pub struct WsScheduleConnector(TestWebSocketClient);

impl HasFixtureClient for TcpScheduleConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsScheduleConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait ScheduleConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl ScheduleConnector for TcpScheduleConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpScheduleConnector)
    }
}

#[async_trait::async_trait]
impl ScheduleConnector for WsScheduleConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsScheduleConnector)
    }
}

/// Build SCHEDULE CREATE frame (`msg_type` 700)
pub fn build_schedule_create(route: &str, cron: &str, payload: &[u8]) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string route][string cron][u8 mode][bytes payload]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    // Cron expression (length-prefixed string)
    buf.put_u32(u32_len(cron.len()));
    buf.put_slice(cron.as_bytes());
    buf.put_u8(fitz::domains::schedule::ScheduleDeliveryMode::Broadcast as u8);

    // Payload (length-prefixed bytes)
    buf.put_u32(u32_len(payload.len()));
    buf.put_slice(payload);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(700, &buf);
    builder.build()
}

/// Build SCHEDULE CREATE BATCH frame (`msg_type` 706)
pub fn build_schedule_create_batch(entries: &[(&str, &str, &[u8])]) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(u32_len(entries.len()));

    for (route, cron, payload) in entries {
        buf.put_u32(u32_len(route.len()));
        buf.put_slice(route.as_bytes());

        buf.put_u32(u32_len(cron.len()));
        buf.put_slice(cron.as_bytes());
        buf.put_u8(fitz::domains::schedule::ScheduleDeliveryMode::Broadcast as u8);

        buf.put_u32(u32_len(payload.len()));
        buf.put_slice(payload);
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(706, &buf);
    builder.build()
}

/// Build SCHEDULE CANCEL frame (`msg_type` 701)
pub fn build_schedule_cancel(route: &str) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string route]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(701, &buf);
    builder.build()
}

/// Build SCHEDULE LIST frame (`msg_type` 702)
pub fn build_schedule_list() -> Vec<u8> {
    // Wire format: empty payload
    let builder = TlvFrameBuilder::new();
    let mut frame_builder = builder;
    frame_builder.encode_field(702, &[]);
    frame_builder.build()
}

/// Build SCHEDULE SUBSCRIBE frame (`msg_type` 703)
pub fn build_schedule_subscribe(route_pattern: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(u32_len(route_pattern.len()));
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(703, &buf);
    builder.build()
}

/// Build SCHEDULE UNSUBSCRIBE frame (`msg_type` 704)
pub fn build_schedule_unsubscribe(route_pattern: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(u32_len(route_pattern.len()));
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(704, &buf);
    builder.build()
}

/// Parse SCHEDULE response
pub fn parse_schedule_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][...response data...]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        // Return msg_type (as u8), status, and full payload for further parsing
        return (msg_type_to_u8(msg_type), status, payload);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

pub struct ScheduleDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub route: String,
    pub body: Vec<u8>,
}

pub fn parse_schedule_delivery(frame: &[u8]) -> Result<ScheduleDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing schedule delivery frame".to_string())?;
    if msg_type != 705 {
        return Err(format!("Unexpected schedule delivery msg_type: {msg_type}"));
    }

    let mut dec = PayloadDecoder::new(&payload);
    let subscription_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let body = dec.get_bytes()?.to_vec();
    if !dec.is_complete() {
        return Err("Trailing data in schedule delivery".to_string());
    }

    Ok(ScheduleDelivery {
        msg_type,
        subscription_id,
        route,
        body,
    })
}
