use super::*;

// ============================================================================
// QUEUE DOMAIN - CONNECTOR TRAIT
// ============================================================================

pub struct TcpQueueConnector(TestClient);
pub struct WsQueueConnector(TestWebSocketClient);

impl HasFixtureClient for TcpQueueConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsQueueConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait QueueConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl QueueConnector for TcpQueueConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpQueueConnector)
    }
}

#[async_trait::async_trait]
impl QueueConnector for WsQueueConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsQueueConnector)
    }
}

/// Build QUEUE ENQUEUE frame (`msg_type` 200)
fn normalize_queue_route(queue_name: &str) -> String {
    if queue_name.contains("://") {
        queue_name.to_string()
    } else {
        format!("queue://test/app/{queue_name}")
    }
}

fn normalize_queue_watch_pattern(pattern: &str) -> String {
    normalize_queue_route(pattern)
}

pub fn build_queue_enqueue(queue_name: &str, data: &[u8]) -> Vec<u8> {
    // Wire format: [u32 route_len][route][u32 body_len][body][u8 has_delay=0]
    let route = normalize_queue_route(queue_name);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(u32_len(route.len())).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&(u32_len(data.len())).to_be_bytes());
    payload.extend_from_slice(data);
    payload.push(0); // has_delay = false

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(200, &payload);
    builder.build()
}

/// Build QUEUE RESERVE frame (`msg_type` 202)
pub fn build_queue_dequeue(queue_name: &str) -> Vec<u8> {
    // Wire format: [u32 route_len][route][u64 lease_seconds][u8 has_batch=0]
    let route = normalize_queue_route(queue_name);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(u32_len(route.len())).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&30_u64.to_be_bytes()); // lease_seconds = 30
    payload.push(0); // has_batch_size = false

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(202, &payload);
    builder.build()
}

/// Build QUEUE WATCH frame (`msg_type` 207).
pub fn build_queue_watch(pattern: &str) -> Vec<u8> {
    let pattern = normalize_queue_watch_pattern(pattern);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(u32_len(pattern.len())).to_be_bytes());
    payload.extend_from_slice(pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(207, &payload);
    builder.build()
}

/// Build QUEUE UNWATCH frame (`msg_type` 208).
pub fn build_queue_unwatch(pattern: &str) -> Vec<u8> {
    let pattern = normalize_queue_watch_pattern(pattern);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(u32_len(pattern.len())).to_be_bytes());
    payload.extend_from_slice(pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(208, &payload);
    builder.build()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueWatchDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub route: String,
    pub ready_messages: u64,
    pub delayed_messages: u64,
    pub inflight_messages: u64,
}

pub fn extract_queue_subscription_id(data: &[u8]) -> Result<u64, String> {
    if data.len() < 9 {
        return Err("Queue watch response too short".to_string());
    }

    Ok(u64::from_be_bytes(data[1..9].try_into().map_err(|_| {
        "Queue watch response missing subscription id".to_string()
    })?))
}

pub fn parse_queue_watch_delivery(frame: &[u8]) -> Result<QueueWatchDelivery, String> {
    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field_ref()
        .ok_or_else(|| "Missing queue watch delivery frame".to_string())?;
    if msg_type != 209 {
        return Err(format!("Unexpected queue watch msg_type: {msg_type}"));
    }

    if payload.len() < 36 {
        return Err("Queue watch payload too short".to_string());
    }

    let subscription_id = u64::from_be_bytes(payload[0..8].try_into().unwrap());
    let route_len = u32_to_usize(u32::from_be_bytes(payload[8..12].try_into().unwrap()));
    if payload.len() < 12 + route_len + 24 {
        return Err("Queue watch payload truncated".to_string());
    }

    let route = String::from_utf8(payload[12..12 + route_len].to_vec())
        .map_err(|_| "Queue watch route is not valid UTF-8".to_string())?;
    let offset = 12 + route_len;
    let ready_messages = u64::from_be_bytes(payload[offset..offset + 8].try_into().unwrap());
    let delayed_messages = u64::from_be_bytes(payload[offset + 8..offset + 16].try_into().unwrap());
    let inflight_messages =
        u64::from_be_bytes(payload[offset + 16..offset + 24].try_into().unwrap());

    Ok(QueueWatchDelivery {
        msg_type,
        subscription_id,
        route,
        ready_messages,
        delayed_messages,
        inflight_messages,
    })
}

/// Parse QUEUE response
pub fn parse_queue_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
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

/// Extract message bodies from `QUEUE_RESERVE` response
/// Wire format: [`u8 status`][`u32 count`][for each: `u64 id`, `u64 token`, `u32 body_len`, bytes body]
pub fn extract_queue_messages(data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if data.len() < 5 {
        return Err("Queue response data too short".to_string());
    }

    // Byte 0: status (already checked by caller)
    // Bytes 1-4: message count
    let count = u32_to_usize(u32::from_be_bytes([data[1], data[2], data[3], data[4]]));

    let mut messages = Vec::new();
    let mut offset = 5;

    for _ in 0..count {
        if offset + 8 > data.len() {
            return Err("Incomplete message ID".to_string());
        }
        // Skip message ID (8 bytes)
        offset += 8;

        if offset + 8 > data.len() {
            return Err("Incomplete token".to_string());
        }
        // Skip token (8 bytes)
        offset += 8;

        if offset + 4 > data.len() {
            return Err("Incomplete body length".to_string());
        }
        // Read body length
        let body_len = u32_to_usize(u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]));
        offset += 4;

        if offset + body_len > data.len() {
            return Err(format!(
                "Incomplete message body: expected {} bytes, got {}",
                body_len,
                data.len() - offset
            ));
        }

        messages.push(data[offset..offset + body_len].to_vec());
        offset += body_len;
    }

    Ok(messages)
}
