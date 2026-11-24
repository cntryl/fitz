use sha2::{Digest, Sha256};

pub(super) fn strong_etag(bytes: &[u8]) -> String {
    format!("\"{}\"", hex::encode(Sha256::digest(bytes)))
}
