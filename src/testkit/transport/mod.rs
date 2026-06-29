mod clients_and_frames;
mod server;

pub(super) const TEST_ISSUER: &str = "https://idp.example";
pub(super) const TEST_AUDIENCE: &str = "fitz-broker";
pub(super) const TEST_RUNTIME_AUTH_SECRET: &str = "test-secret-key";

pub use clients_and_frames::{
    build_connect_frame, generate_expired_jwt, generate_invalid_signature_jwt, generate_test_jwt,
    generate_test_jwt_for_family, TestClient, TestWebSocketClient, TlvFrameBuilder, TlvFrameParser,
};
pub use server::TestServer;

#[cfg(test)]
mod tests;
