use bytes::BufMut;
use fitz_core::testkit::frames::FrameBuilder;

#[inline]
fn u32_len(value: usize) -> u32 {
    u32::try_from(value)
        .unwrap_or_else(|_| panic!("transport frame length exceeds u32::MAX: {value}"))
}

/// Build RPC REQUEST frame (`msg_type` 302)
#[must_use]
pub fn build_rpc_request(route: &str, payload: &[u8]) -> Vec<u8> {
    use uuid::Uuid;

    let mut buf = Vec::new();
    let uuid = Uuid::new_v4();
    buf.put_slice(uuid.as_bytes());

    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    buf.put_u32(u32_len(payload.len()));
    buf.put_slice(payload);
    buf.put_u8(0); // no caller budget

    let mut builder = FrameBuilder::new();
    builder.encode_field(302, &buf);
    builder.build()
}
