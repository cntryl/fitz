use super::*;

// ============================================================================
// RPC DOMAIN - CONNECTOR TRAIT
// ============================================================================

pub struct TcpRpcConnector(TestClient);
pub struct WsRpcConnector(TestWebSocketClient);

impl HasFixtureClient for TcpRpcConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsRpcConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait RpcConnector: TestConnectorClient + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl RpcConnector for TcpRpcConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpRpcConnector)
    }
}

#[async_trait::async_trait]
impl RpcConnector for WsRpcConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsRpcConnector)
    }
}

/// Build RPC SUBSCRIBE frame (`msg_type` 300) to register a worker
pub fn build_rpc_subscribe(worker_addr: &str) -> Vec<u8> {
    build_rpc_subscribe_with_max_concurrent(worker_addr, 1)
}

/// Build RPC SUBSCRIBE frame (`msg_type` 300) with explicit worker credit.
pub fn build_rpc_subscribe_with_max_concurrent(worker_addr: &str, max_concurrent: u32) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string worker_addr][u32 max_concurrent]
    let mut buf = Vec::new();
    buf.put_u32(u32_len(worker_addr.len()));
    buf.put_slice(worker_addr.as_bytes());
    buf.put_u32(max_concurrent);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(300, &buf);
    builder.build()
}

/// Build RPC UNSUBSCRIBE frame (`msg_type` 301) to unregister a worker
pub fn build_rpc_unsubscribe(worker_addr: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(u32_len(worker_addr.len()));
    buf.put_slice(worker_addr.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(301, &buf);
    builder.build()
}

/// Build RPC REQUEST frame (`msg_type` 302)
pub fn build_rpc_request(route: &str, _method: &str, payload: &[u8]) -> Vec<u8> {
    use bytes::BufMut;
    use uuid::Uuid;

    // Wire format: [uuid16 correlation_id][string route][bytes body]
    let mut buf = Vec::new();

    let uuid = Uuid::new_v4();
    buf.put_slice(uuid.as_bytes());

    // Route (length-prefixed string)
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    // Body (length-prefixed bytes)
    buf.put_u32(u32_len(payload.len()));
    buf.put_slice(payload);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(302, &buf);
    builder.build()
}

/// Build RPC RESPONSE frame for workers (`msg_type` 303)
pub fn build_rpc_response_delivery(
    correlation_id: uuid::Uuid,
    seq: u64,
    stream_end: bool,
    body: &[u8],
) -> Vec<u8> {
    use fitz::protocol::payload_codec::PayloadEncoder;

    let mut enc = PayloadEncoder::new();
    let uuid_bytes = correlation_id.as_bytes();
    enc.put_u64(u64::from_be_bytes(uuid_bytes[..8].try_into().unwrap()));
    enc.put_u64(u64::from_be_bytes(uuid_bytes[8..].try_into().unwrap()));
    enc.put_u64(seq);
    enc.put_u8(u8::from(stream_end));
    enc.put_bytes(body);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(303, &enc.finish());
    builder.build()
}

/// Parse RPC response
pub fn parse_rpc_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
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

pub struct RpcRequestDelivery {
    pub msg_type: u16,
    pub correlation_id: uuid::Uuid,
    pub route: String,
    pub body: Vec<u8>,
}

pub struct RpcResponseDelivery {
    pub msg_type: u16,
    pub correlation_id: uuid::Uuid,
    pub seq: u64,
    pub body: Vec<u8>,
    pub stream_end: bool,
}

/// Parse RPC REQUEST delivery (`msg_type` 302) sent to workers
pub fn parse_rpc_request_delivery(frame: &[u8]) -> Result<RpcRequestDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing RPC request delivery frame".to_string())?;
    if msg_type != 302 {
        return Err(format!("Unexpected RPC request msg_type: {msg_type}"));
    }

    let mut dec = PayloadDecoder::new(&payload);
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes[..8].copy_from_slice(&dec.get_u64()?.to_be_bytes());
    uuid_bytes[8..].copy_from_slice(&dec.get_u64()?.to_be_bytes());
    let correlation_id = uuid::Uuid::from_bytes(uuid_bytes);

    let route = dec.get_string()?;
    let body = dec.get_bytes()?.to_vec();
    if !dec.is_complete() {
        return Err("Trailing data in RPC request delivery".to_string());
    }

    Ok(RpcRequestDelivery {
        msg_type,
        correlation_id,
        route,
        body,
    })
}

/// Parse RPC RESPONSE delivery (`msg_type` 303) received by callers
pub fn parse_rpc_response_delivery(frame: &[u8]) -> Result<RpcResponseDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing RPC response delivery frame".to_string())?;
    if msg_type != 303 {
        return Err(format!("Unexpected RPC response msg_type: {msg_type}"));
    }

    let mut dec = PayloadDecoder::new(&payload);
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes[..8].copy_from_slice(&dec.get_u64()?.to_be_bytes());
    uuid_bytes[8..].copy_from_slice(&dec.get_u64()?.to_be_bytes());
    let correlation_id = uuid::Uuid::from_bytes(uuid_bytes);

    let seq = dec.get_u64()?;
    let flags = dec.get_u8()?;
    let body = dec.get_bytes()?.to_vec();
    let stream_end = flags & 0x01 != 0;
    if !dec.is_complete() {
        return Err("Trailing data in RPC response delivery".to_string());
    }

    Ok(RpcResponseDelivery {
        msg_type,
        correlation_id,
        seq,
        body,
        stream_end,
    })
}
