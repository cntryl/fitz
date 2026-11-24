//! Checksum-valid malformed artifacts for testing untrusted decoder boundaries.

use super::{checksum_payload, SnapshotArtifact, SnapshotDomain};

pub(crate) fn with_resource_route(
    artifact: &SnapshotArtifact,
    index: usize,
    route: &str,
) -> Vec<u8> {
    let mut envelope = artifact.envelope.clone();
    match envelope.payload.domain {
        SnapshotDomain::Kv => envelope.payload.resources[index].route = route.to_string(),
        SnapshotDomain::Stream => {
            envelope.payload.stream_resources[index].route = route.to_string();
        }
    }
    envelope.checksum_sha256 = checksum_payload(&envelope.payload).expect("fixture checksum");
    serde_json::to_vec(&envelope).expect("fixture bytes")
}
