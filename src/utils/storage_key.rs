use lexkey::{Encoder, LexKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainKeyspace {
    Kv,
    Lease,
    Notice,
    Queue,
    Rpc,
    Schedule,
    Stream,
}

impl DomainKeyspace {
    #[must_use]
    pub fn code(self) -> [u8; 2] {
        match self {
            Self::Kv => *b"kv",
            Self::Lease => *b"ls",
            Self::Notice => *b"nt",
            Self::Queue => *b"qu",
            Self::Rpc => *b"rp",
            Self::Schedule => *b"sc",
            Self::Stream => *b"st",
        }
    }
}

#[must_use]
pub fn domain_prefix(realm: &str, domain: DomainKeyspace) -> Vec<u8> {
    let domain_code = domain.code();
    let mut encoder = Encoder::with_capacity(realm.len() + domain_code.len() + 2);
    encoder.encode_string_into(realm);
    encoder.push_separator();
    encoder.encode_bytes_into(domain_code.as_slice());
    encoder.push_separator();
    encoder.into_vec()
}

#[must_use]
pub fn realm_domain_prefix(realm: &str, domain: &str) -> Vec<u8> {
    let mut encoder = Encoder::with_capacity(realm.len() + domain.len() + 2);
    encoder.encode_string_into(realm);
    encoder.push_separator();
    encoder.encode_string_into(domain);
    encoder.push_separator();
    encoder.into_vec()
}

#[must_use]
pub fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    LexKey::encode_range_upper(prefix, None).as_bytes().to_vec()
}

#[must_use]
pub fn domain_range(realm: &str, domain: DomainKeyspace) -> (Vec<u8>, Vec<u8>) {
    let prefix = domain_prefix(realm, domain);
    let end = prefix_range_end(&prefix);
    (prefix, end)
}

#[must_use]
pub fn realm_prefix(realm: &str) -> Vec<u8> {
    let mut encoder = Encoder::with_capacity(realm.len() + 1);
    encoder.encode_string_into(realm);
    encoder.push_separator();
    encoder.into_vec()
}

#[must_use]
pub fn domain_marker_encoder(
    realm: &str,
    domain: DomainKeyspace,
    marker: u8,
    extra_capacity: usize,
) -> Encoder {
    let domain_code = domain.code();
    let mut encoder = Encoder::with_capacity(realm.len() + domain_code.len() + extra_capacity + 3);
    encoder.encode_string_into(realm);
    encoder.push_separator();
    encoder.encode_bytes_into(domain_code.as_slice());
    encoder.push_separator();
    encoder.push_byte(marker);
    encoder
}

#[must_use]
pub fn realm_range(realm: &str) -> (Vec<u8>, Vec<u8>) {
    let prefix = realm_prefix(realm);
    let end = prefix_range_end(&prefix);
    (prefix, end)
}

pub fn push_segment(buffer: &mut Vec<u8>, segment: &str) {
    buffer.extend_from_slice(segment.as_bytes());
    buffer.push(LexKey::SEPARATOR);
}

pub fn encode_segment_into(encoder: &mut Encoder, segment: &str) {
    encoder.encode_string_into(segment);
    encoder.push_separator();
}

pub fn push_bytes_segment(buffer: &mut Vec<u8>, segment: &[u8]) {
    buffer.extend_from_slice(segment);
    buffer.push(LexKey::SEPARATOR);
}

pub fn encode_bytes_segment_into(encoder: &mut Encoder, segment: &[u8]) {
    encoder.encode_bytes_into(segment);
    encoder.push_separator();
}

#[must_use]
pub fn prefixed_key(realm: &str, domain: DomainKeyspace, suffix: &[u8]) -> Vec<u8> {
    let mut key = domain_prefix(realm, domain);
    key.reserve(suffix.len());
    key.extend_from_slice(suffix);
    key
}

#[must_use]
pub fn split_domain_key(key: &[u8], domain: DomainKeyspace) -> Option<(&str, &[u8])> {
    let realm_end = key.iter().position(|byte| *byte == LexKey::SEPARATOR)?;
    let realm = std::str::from_utf8(&key[..realm_end]).ok()?;
    let suffix = strip_domain_prefix(key, domain)?;
    Some((realm, suffix))
}

#[must_use]
pub fn strip_realm_domain_prefix<'a>(key: &'a [u8], domain: &str) -> Option<&'a [u8]> {
    let realm_end = key.iter().position(|byte| *byte == LexKey::SEPARATOR)?;
    let domain_start = realm_end.saturating_add(1);
    let domain_end_rel = key[domain_start..]
        .iter()
        .position(|byte| *byte == LexKey::SEPARATOR)?;
    let domain_end = domain_start + domain_end_rel;

    if &key[domain_start..domain_end] != domain.as_bytes() {
        return None;
    }

    Some(&key[domain_end + 1..])
}

#[must_use]
pub fn strip_domain_prefix(key: &[u8], domain: DomainKeyspace) -> Option<&[u8]> {
    let domain_code = domain.code();
    let realm_end = key.iter().position(|byte| *byte == LexKey::SEPARATOR)?;
    let domain_start = realm_end.saturating_add(1);
    let domain_end = domain_start.saturating_add(2);

    if key.len() <= domain_end || key.get(domain_end) != Some(&LexKey::SEPARATOR) {
        return None;
    }

    if key.get(domain_start..domain_end)? != domain_code.as_slice() {
        return None;
    }

    Some(&key[domain_end + 1..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    use crate::testkit::create_test_engine_with_cfs;

    #[test]
    fn should_encode_realm_domain_prefix() {
        // Arrange
        let realm = "acme";
        let domain = "kv";

        // Act
        let prefix = realm_domain_prefix(realm, domain);

        // Assert
        assert_eq!(prefix, b"acme\0kv\0".to_vec());
    }

    #[test]
    fn should_compute_exclusive_prefix_range_end() {
        // Arrange
        let prefix = b"acme\0kv\0users\0";

        // Act
        let range_end = prefix_range_end(prefix);

        // Assert
        assert!(range_end.as_slice() > prefix.as_slice());
        assert!(b"acme\0kv\0users\0x".as_slice() < range_end.as_slice());
    }

    #[test]
    fn should_strip_realm_domain_prefix() {
        // Arrange
        let key = b"acme\0schedule\0def\0jobs";

        // Act
        let suffix = strip_realm_domain_prefix(key, "schedule");

        // Assert
        assert_eq!(suffix, Some(b"def\0jobs".as_slice()));
    }

    #[test]
    fn should_encode_domain_prefix_from_keyspace() {
        // Arrange
        let realm = "acme";

        // Act
        let prefix = domain_prefix(realm, DomainKeyspace::Queue);

        // Assert
        assert_eq!(prefix, b"acme\0qu\0".to_vec());
    }

    #[test]
    fn should_prefix_key_with_domain_namespace() {
        // Arrange
        let suffix = b"hdr\0jobs\0";

        // Act
        let key = prefixed_key("acme", DomainKeyspace::Queue, suffix);

        // Assert
        assert_eq!(key, b"acme\0qu\0hdr\0jobs\0".to_vec());
    }

    #[test]
    fn should_encode_domain_marker_prefix() {
        // Arrange
        let mut encoder = domain_marker_encoder("acme", DomainKeyspace::Schedule, 0x03, 16);

        // Act
        encode_segment_into(&mut encoder, "jobs");
        encoder.encode_u64_into(42);
        let key = encoder.into_vec();

        // Assert
        assert_eq!(key, b"acme\0sc\0\x03jobs\0\0\0\0\0\0\0\0*".to_vec());
    }

    #[test]
    fn should_split_domain_key_with_realm() {
        // Arrange
        let key = prefixed_key("acme", DomainKeyspace::Queue, b"jobs\0email\0\x04payload");

        // Act
        let split = split_domain_key(&key, DomainKeyspace::Queue);

        // Assert
        assert_eq!(
            split,
            Some(("acme", b"jobs\0email\0\x04payload".as_slice()))
        );
    }

    #[test]
    fn should_append_separator_after_string_segment() {
        // Arrange
        let mut buffer = Vec::new();

        // Act
        push_segment(&mut buffer, "users");

        // Assert
        assert_eq!(buffer, b"users\0".to_vec());
    }

    #[test]
    fn should_append_separator_after_bytes_segment() {
        // Arrange
        let mut buffer = Vec::new();

        // Act
        push_bytes_segment(&mut buffer, b"\x01jobs");

        // Assert
        assert_eq!(buffer, b"\x01jobs\0".to_vec());
    }

    #[test]
    fn should_scan_multiple_domains_within_realm_range() {
        // Arrange
        let engine = create_test_engine_with_cfs(vec![1]);
        let mut tx = engine
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin write tx");
        let keys = [
            prefixed_key("acme", DomainKeyspace::Kv, b"users\0profile\0alpha"),
            prefixed_key("acme", DomainKeyspace::Queue, b"jobs:mailer:meta"),
            prefixed_key(
                "acme",
                DomainKeyspace::Schedule,
                b"sched:def:schedule://acme/jobs/nightly/run",
            ),
            prefixed_key("acme", DomainKeyspace::Stream, b"\x08orders\0checkout"),
            prefixed_key("beta", DomainKeyspace::Kv, b"users\0profile\0beta"),
        ];

        for (index, key) in keys.iter().enumerate() {
            tx.put(key.clone(), vec![index as u8], None)
                .expect("write test key");
        }
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit test keys");

        let (start, end) = realm_range("acme");
        let tx = engine
            .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");

        // Act
        let rows = tx
            .scan(
                &cntryl_midge::Query::new()
                    .start_key(Bytes::from(start))
                    .end_key(Bytes::from(end)),
            )
            .expect("scan realm range")
            .collect_all();

        // Assert
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|(key, _)| key.starts_with(b"acme\0")));
        assert!(rows
            .iter()
            .any(|(key, _)| strip_domain_prefix(key, DomainKeyspace::Kv).is_some()));
        assert!(rows
            .iter()
            .any(|(key, _)| strip_domain_prefix(key, DomainKeyspace::Queue).is_some()));
        assert!(rows
            .iter()
            .any(|(key, _)| strip_domain_prefix(key, DomainKeyspace::Schedule).is_some()));
        assert!(rows
            .iter()
            .any(|(key, _)| strip_domain_prefix(key, DomainKeyspace::Stream).is_some()));
    }
}
