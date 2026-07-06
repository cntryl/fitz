use super::*;

// ============================================================================
// KV DOMAIN - CONNECTOR TRAIT
// ============================================================================

#[async_trait::async_trait]
pub trait KvConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl KvConnector for TcpClient {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpClient)
    }
}

#[async_trait::async_trait]
impl KvConnector for WsClient {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsClient)
    }
}

// Type aliases for backwards compatibility with test code
pub type TcpConnector = TcpClient;
pub type WsConnector = WsClient;

/// Build KV BEGIN frame (`msg_type` 100)
pub fn build_kv_begin(route: &str, mode: u8, durability: u8) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u32 BE route_len][route][u8 mode][u8 durability]
    payload.extend_from_slice(&(u32_len(route.len())).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.push(mode);
    payload.push(durability);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(100, &payload);
    builder.build()
}

/// Build KV PUT frame (`msg_type` 104)
pub fn build_kv_put(tx_id: u64, route: &str, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u64 BE tx_id][u32 BE route_len][route][u32 BE key_len][key][u32 BE value_len][value]
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(u32_len(route.len())).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&(u32_len(key.len())).to_be_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(u32_len(value.len())).to_be_bytes());
    payload.extend_from_slice(value);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(104, &payload);
    builder.build()
}

/// Build KV GET frame (`msg_type` 103)
pub fn build_kv_get(tx_id: u64, route: &str, key: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u64 BE tx_id][u32 BE route_len][route][u32 BE key_len][key]
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(u32_len(route.len())).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&(u32_len(key.len())).to_be_bytes());
    payload.extend_from_slice(key);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(103, &payload);
    builder.build()
}

/// Build KV COMMIT frame (`msg_type` 101)
pub fn build_kv_commit(tx_id: u64, route: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u64 BE tx_id][u32 BE route_len][route]
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(u32_len(route.len())).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(101, &payload);
    builder.build()
}

/// Build KV SUBSCRIBE frame (`msg_type` 109)
pub fn build_kv_subscribe(route_pattern: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(u32_len(route_pattern.len())).to_be_bytes());
    payload.extend_from_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(109, &payload);
    builder.build()
}

/// Build KV UNSUBSCRIBE frame (`msg_type` 110)
pub fn build_kv_unsubscribe(route_pattern: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(u32_len(route_pattern.len())).to_be_bytes());
    payload.extend_from_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(110, &payload);
    builder.build()
}

/// Build KV ROLLBACK frame (`msg_type` 102)
pub fn build_kv_rollback(tx_id: u64, route: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u64 BE tx_id][u32 BE route_len][route]
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(u32_len(route.len())).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(102, &payload);
    builder.build()
}

/// Parse KV response
/// Format: [u8 status][optional data...]
pub fn parse_kv_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][...response data...]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        // Return msg_type (as u8), status, and full payload (including status byte)
        // Helper functions expect the full payload and will skip the status byte themselves
        return (msg_type_to_u8(msg_type), status, payload);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

/// Parse KV transaction ID from response (big-endian u64)
pub fn parse_kv_tx_id(data: &[u8]) -> Result<u64, String> {
    // BeginOk format: [u8 status][u64 tx_id]
    // Skip status byte at data[0], read tx_id from data[1..9]
    if data.len() < 9 {
        return Err(format!(
            "TX ID data too short: {} bytes, need 9",
            data.len()
        ));
    }
    let bytes = [
        data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
    ];
    Ok(u64::from_be_bytes(bytes))
}

pub fn extract_kv_subscription_id(data: &[u8]) -> Result<u64, String> {
    if data.len() < 9 {
        return Err("KV subscribe response too short".to_string());
    }

    Ok(u64::from_be_bytes(data[1..9].try_into().map_err(|_| {
        "KV subscribe response missing subscription id".to_string()
    })?))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvWatchDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub route: String,
    pub mutation_count: u64,
}

pub fn parse_kv_watch_delivery(frame: &[u8]) -> Result<KvWatchDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing KV watch delivery frame".to_string())?;
    if msg_type != 111 {
        return Err(format!("Unexpected KV watch delivery msg_type: {msg_type}"));
    }

    let mut decoder = PayloadDecoder::new(&payload);
    let subscription_id = decoder.get_u64()?;
    let route = decoder.get_string()?;
    let mutation_count = decoder.get_u64()?;
    if !decoder.is_complete() {
        return Err("Trailing data in KV watch delivery".to_string());
    }

    Ok(KvWatchDelivery {
        msg_type,
        subscription_id,
        route,
        mutation_count,
    })
}

/// Extract value from `KV GET` response
///
/// `GetResult` format: [`u8 status`][`u8 found`][`u32 length_be`][...`value_bytes`]
/// Returns the actual value bytes if found, empty vec if not found
pub fn extract_kv_value(data: &[u8]) -> Result<Vec<u8>, String> {
    if data.len() < 6 {
        return Err("GetResult data too short".to_string());
    }

    let found = data[1];
    if found == 0 {
        return Ok(Vec::new()); // Not found
    }

    // Read length from bytes 2-5 (big-endian u32)
    let length = u32_to_usize(u32::from_be_bytes([data[2], data[3], data[4], data[5]]));

    // Extract value from bytes 6 onwards
    if data.len() < 6 + length {
        return Err(format!(
            "GetResult value incomplete: expected {} bytes, got {}",
            length,
            data.len() - 6
        ));
    }

    Ok(data[6..6 + length].to_vec())
}

/// Parse KV value from response
pub fn parse_kv_value(data: &[u8]) -> Vec<u8> {
    data.to_vec()
}
