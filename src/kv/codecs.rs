//! Encoding/decoding for KV operations.

pub trait KvCodec<T> {
    fn encode(&self, value: &T) -> Vec<u8>;
    fn decode(&self, bytes: &[u8]) -> Result<T, String>;
}
