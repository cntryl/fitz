use super::*;

// ============================================================================
// STREAM DOMAIN - CONNECTOR TRAIT
// ============================================================================

pub struct TcpStreamConnector(TestClient);
pub struct WsStreamConnector(TestWebSocketClient);

impl HasFixtureClient for TcpStreamConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsStreamConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait StreamConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl StreamConnector for TcpStreamConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpStreamConnector)
    }
}

#[async_trait::async_trait]
impl StreamConnector for WsStreamConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsStreamConnector)
    }
}

/// Build STREAM BEGIN frame (`msg_type` 600)
/// Wire format: [string route][optional bytes ingest_metadata]
pub fn build_stream_begin(route: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    // Optional ingest metadata (flag = 0 for none)
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(600, &buf);
    builder.build()
}

/// Build STREAM APPEND frame (`msg_type` 601)
pub fn build_stream_append(session_id: u64, expected_offset: u64, data: &[u8]) -> Vec<u8> {
    build_stream_append_with_metadata(session_id, expected_offset, data, None)
}

/// Build STREAM APPEND frame (`msg_type` 601) with optional metadata.
pub fn build_stream_append_with_metadata(
    session_id: u64,
    expected_offset: u64,
    data: &[u8],
    metadata: Option<&[u8]>,
) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [u64 session_id][u64 expected_offset][bytes body][optional metadata]
    let mut buf = Vec::new();

    // Session ID
    buf.put_u64(session_id);

    // Expected offset
    buf.put_u64(expected_offset);

    // Body (length-prefixed bytes)
    buf.put_u32(u32_len(data.len()));
    buf.put_slice(data);

    match metadata {
        Some(metadata) => {
            buf.put_u8(1);
            buf.put_u32(u32_len(metadata.len()));
            buf.put_slice(metadata);
        }
        None => buf.put_u8(0),
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(601, &buf);
    builder.build()
}

/// Build STREAM COMMIT frame (`msg_type` 602)
pub fn build_stream_commit(session_id: u64) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u64(session_id);
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(602, &buf);
    builder.build()
}

/// Build STREAM SUBSCRIBE frame (`msg_type` 607)
pub fn build_stream_subscribe(route_pattern: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(u32_len(route_pattern.len()));
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(607, &buf);
    builder.build()
}

/// Build STREAM UNSUBSCRIBE frame (`msg_type` 608)
pub fn build_stream_unsubscribe(route_pattern: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(u32_len(route_pattern.len()));
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(608, &buf);
    builder.build()
}

/// Build STREAM APPEND frame with default session ID (for simple tests)
/// Uses `session_id = 1` by default; tests should call `BEGIN` first if they need a real session
pub fn build_stream_append_simple(_route: &str, data: &[u8]) -> Vec<u8> {
    build_stream_append(1, 0, data)
}

/// Build STREAM READ frame (`msg_type` 604)
pub fn build_stream_read(route: &str, start_offset: u64) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string route][u64 from_offset][u64 limit][optional max_bytes]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    // From offset
    buf.put_u64(start_offset);

    // Limit (read up to 1000 entries)
    buf.put_u64(1000);

    // Optional max_bytes (flag = 0 for none)
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(604, &buf);
    builder.build()
}

/// Build STREAM LAST frame (`msg_type` 605)
pub fn build_stream_last(route: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(605, &buf);
    builder.build()
}

/// Build STREAM `GET_METADATA` frame (`msg_type` 606)
pub fn build_stream_get_metadata(route: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(606, &buf);
    builder.build()
}

/// Parse `STREAM` response
pub fn parse_stream_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][optional u64 session_id][...response data...]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        // Return msg_type (as u8), status, and full payload for further parsing
        return (msg_type_to_u8(msg_type), status, payload);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

/// Parse `session_id` from `STREAM` `BEGIN` response data
/// Wire format: [`u8 status`][`u64 session_id`][bytes data]
pub fn parse_stream_session_id(data: &[u8]) -> Result<u64, String> {
    if data.len() < 10 {
        return Err("Stream response data too short".to_string());
    }

    // Byte 0: status (0 = success, 1 = error)
    let status = data[0];
    if status != 0 {
        return Err("Stream BEGIN operation failed".to_string());
    }

    let (session_id, payload_offset) = if data[1] == 1 && data.len() >= 14 {
        (
            u64::from_be_bytes(data[2..10].try_into().expect("checked length")),
            10,
        )
    } else {
        (
            u64::from_be_bytes(data[1..9].try_into().expect("checked length")),
            9,
        )
    };
    if data.len() < payload_offset + 4 {
        return Err("Stream response data too short".to_string());
    }
    let payload_len = u32::from_be_bytes(
        data[payload_offset..payload_offset + 4]
            .try_into()
            .expect("checked length"),
    ) as usize;
    if data.len() != payload_offset + 4 + payload_len {
        return Err("Invalid Stream BEGIN payload length".to_string());
    }
    Ok(session_id)
}

pub struct StreamDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub route: String,
    pub body: Vec<u8>,
}

pub fn parse_stream_delivery(frame: &[u8]) -> Result<StreamDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing stream delivery frame".to_string())?;
    if msg_type != 609 {
        return Err(format!("Unexpected stream delivery msg_type: {msg_type}"));
    }

    let mut dec = PayloadDecoder::new(&payload);
    let subscription_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let body = dec.get_bytes()?.to_vec();
    if !dec.is_complete() {
        return Err("Trailing data in stream delivery".to_string());
    }

    Ok(StreamDelivery {
        msg_type,
        subscription_id,
        route,
        body,
    })
}
