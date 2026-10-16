//! Family-scoped scans that avoid reading other domains' payload ranges.

use crate::utils::storage_key::{prefix_range_end, strip_domain_prefix, DomainKeyspace};
use bytes::Bytes;
use cntryl_midge::{MidgeResult, Query, Transaction};

/// Scan selected owned row kinds while preserving unknown/legacy namespaces.
/// Non-selected owned kinds are skipped with a range seek, not row filtering.
pub(crate) fn scan_domain_marker_rows<'a>(
    transaction: &'a Transaction,
    domain: DomainKeyspace,
    include_marker: impl Fn(u8) -> bool + 'a,
) -> MidgeResult<impl Iterator<Item = MidgeResult<(Bytes, Bytes)>> + 'a> {
    scan_rows(transaction, domain, include_marker)
}

/// Scan owned and unrecognized rows from one transaction snapshot.
///
/// Unrecognized and legacy binary-leading rows remain visible for corruption
/// validation. Foreign realm/domain ranges, including the empty internal
/// realm used by Stream payload blobs, cost one row and a seek.
pub(crate) fn scan_domain_rows(
    transaction: &Transaction,
    domain: DomainKeyspace,
) -> MidgeResult<impl Iterator<Item = MidgeResult<(Bytes, Bytes)>> + '_> {
    scan_rows(transaction, domain, |_| true)
}

fn scan_rows<'a>(
    transaction: &'a Transaction,
    domain: DomainKeyspace,
    include_marker: impl Fn(u8) -> bool + 'a,
) -> MidgeResult<impl Iterator<Item = MidgeResult<(Bytes, Bytes)>> + 'a> {
    let mut rows = Some(transaction.scan(&Query::new())?);
    Ok(std::iter::from_fn(move || loop {
        let row = rows.as_mut()?.next()?;
        let (key, value) = match row {
            Ok(row) => row,
            Err(error) => {
                rows = None;
                return Some(Err(error));
            }
        };
        let end = foreign_domain_end(&key, domain)
            .or_else(|| unselected_marker_end(&key, domain, &include_marker));
        let Some(end) = end else {
            return Some(Ok((key, value)));
        };
        // An unwanted row identifies its domain or row-kind range. Seek past
        // that range instead of materializing every irrelevant payload.
        drop(value);
        rows = None;
        match transaction.scan(&Query::new().start_key(Bytes::from(end))) {
            Ok(next) => rows = Some(next),
            Err(error) => return Some(Err(error)),
        }
    }))
}

fn unselected_marker_end(
    key: &[u8],
    domain: DomainKeyspace,
    include_marker: &impl Fn(u8) -> bool,
) -> Option<Vec<u8>> {
    let first = *key.first()?;
    if first != lexkey::LexKey::SEPARATOR && !first.is_ascii_graphic() {
        return None;
    }
    let suffix = strip_domain_prefix(key, domain)?;
    if include_marker(*suffix.first()?) {
        return None;
    }
    Some(prefix_range_end(&key[..=key.len() - suffix.len()]))
}

fn foreign_domain_end(key: &[u8], domain: DomainKeyspace) -> Option<Vec<u8>> {
    // Legacy Stream keys begin with a raw control or high-byte marker. Their
    // binary suffix can resemble a realm/domain prefix by coincidence. Keep
    // them visible; non-ASCII realm prefixes conservatively use the full scan.
    // A leading separator is the empty realm, not a legacy Stream marker.
    // Stream uses it for family-scoped payload blobs and ordering metadata.
    let first = *key.first()?;
    if first != lexkey::LexKey::SEPARATOR && !first.is_ascii_graphic() {
        return None;
    }
    for other in [
        DomainKeyspace::Kv,
        DomainKeyspace::Lease,
        DomainKeyspace::Notice,
        DomainKeyspace::Queue,
        DomainKeyspace::Rpc,
        DomainKeyspace::Schedule,
        DomainKeyspace::Stream,
    ] {
        if other == domain {
            continue;
        }
        if let Some(suffix) = strip_domain_prefix(key, other) {
            return Some(prefix_range_end(&key[..key.len() - suffix.len()]));
        }
    }
    // Keep unknown/legacy keys visible to the owning validator. This helper
    // only skips keyspaces whose ownership is unambiguous.
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::create_test_engine_with_cfs;
    use crate::utils::storage_key::prefixed_key;
    use cntryl_midge::{TransactionMode, WriteOptions};

    fn seeded_engine(keys: &[Vec<u8>]) -> std::sync::Arc<cntryl_midge::Engine> {
        let engine = create_test_engine_with_cfs(vec![1]);
        let mut tx = engine.begin_tx(1, TransactionMode::ReadWrite).unwrap();
        for key in keys {
            tx.put(key.clone(), b"value".to_vec(), None).unwrap();
        }
        tx.commit(WriteOptions::sync()).unwrap();
        engine
    }

    #[test]
    fn should_scan_stream_metadata_without_payload_ranges_across_realms() {
        // Arrange
        let marker = crate::domains::stream::storage::KeyPrefix::ResourceMeta as u8;
        let expected = vec![
            prefixed_key("a", DomainKeyspace::Stream, &[marker, 0]),
            prefixed_key("z", DomainKeyspace::Stream, &[marker, 255]),
        ];
        let mut keys = expected.clone();
        keys.extend([
            crate::domains::stream::storage::encode_payload_blob_key(0),
            crate::domains::stream::storage::encode_payload_blob_key(1),
            prefixed_key("a", DomainKeyspace::Stream, &[marker - 1, 255]),
            prefixed_key("a", DomainKeyspace::Stream, &[marker + 1, 0]),
            prefixed_key("b", DomainKeyspace::Kv, b"other-domain"),
        ]);
        let engine = seeded_engine(&keys);
        let tx = engine.begin_tx(1, TransactionMode::ReadOnly).unwrap();

        // Act
        let actual = scan_domain_marker_rows(&tx, DomainKeyspace::Stream, |value| value == marker)
            .unwrap()
            .map(|row| row.unwrap().0.to_vec())
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(actual, expected);
    }

    #[test]
    fn should_scan_selected_stream_fragment_kinds_without_payload_blobs() {
        // Arrange
        let markers = [
            crate::domains::stream::storage::KeyPrefix::CompactAreaPage as u8,
            crate::domains::stream::storage::KeyPrefix::CompactResourcePage as u8,
        ];
        let expected = vec![
            prefixed_key("a", DomainKeyspace::Stream, &[markers[0], 0]),
            prefixed_key("a", DomainKeyspace::Stream, &[markers[1], 255]),
        ];
        let mut keys = expected.clone();
        keys.push(crate::domains::stream::storage::encode_payload_blob_key(0));
        let engine = seeded_engine(&keys);
        let tx = engine.begin_tx(1, TransactionMode::ReadOnly).unwrap();

        // Act
        let actual = scan_domain_marker_rows(&tx, DomainKeyspace::Stream, |value| {
            markers.contains(&value)
        })
        .unwrap()
        .map(|row| row.unwrap().0.to_vec())
        .collect::<Vec<_>>();

        // Assert
        assert_eq!(actual, expected);
    }

    #[test]
    fn should_preserve_unrecognized_rows_in_marker_scan() {
        // Arrange
        let expected = vec![
            b"\0unknown\0row".to_vec(),
            b"\x05\0st\0legacy".to_vec(),
            b"realm\0st\0".to_vec(),
            b"realm\0unknown\0row".to_vec(),
            b"\xea\0st\0legacy".to_vec(),
        ];
        let engine = seeded_engine(&expected);
        let tx = engine.begin_tx(1, TransactionMode::ReadOnly).unwrap();

        // Act
        let actual = scan_domain_marker_rows(&tx, DomainKeyspace::Stream, |value| {
            value == crate::domains::stream::storage::KeyPrefix::ResourceMeta as u8
        })
        .unwrap()
        .map(|row| row.unwrap().0.to_vec())
        .collect::<Vec<_>>();

        // Assert
        assert_eq!(actual, expected);
    }

    #[test]
    fn should_scan_owned_rows_across_realms_without_foreign_domain_rows() {
        // Arrange
        let expected = vec![
            prefixed_key("a", DomainKeyspace::Queue, b"\0"),
            prefixed_key("a", DomainKeyspace::Queue, b"\xff\xff"),
            prefixed_key("z", DomainKeyspace::Queue, b"last"),
        ];
        let mut keys = expected.clone();
        keys.extend([
            prefixed_key("a", DomainKeyspace::Kv, b"first"),
            prefixed_key("a", DomainKeyspace::Kv, b"\xff\xff"),
            prefixed_key("a", DomainKeyspace::Stream, b"last"),
            prefixed_key("b", DomainKeyspace::Kv, b"only-foreign"),
            prefixed_key("z", DomainKeyspace::Schedule, b"last"),
        ]);
        let engine = seeded_engine(&keys);
        let tx = engine.begin_tx(1, TransactionMode::ReadOnly).unwrap();

        // Act
        let actual = scan_domain_rows(&tx, DomainKeyspace::Queue)
            .unwrap()
            .map(|row| row.unwrap().0.to_vec())
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(actual, expected);
    }

    #[test]
    fn should_skip_foreign_family_scoped_stream_payloads() {
        // Arrange
        let expected = vec![prefixed_key("app", DomainKeyspace::Schedule, b"definition")];
        let mut keys = expected.clone();
        keys.push(crate::domains::stream::storage::encode_payload_blob_key(0));
        let engine = seeded_engine(&keys);
        let tx = engine.begin_tx(1, TransactionMode::ReadOnly).unwrap();

        // Act
        let actual = scan_domain_rows(&tx, DomainKeyspace::Schedule)
            .unwrap()
            .map(|row| row.unwrap().0.to_vec())
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(actual, expected);
    }

    #[test]
    fn should_preserve_owned_family_scoped_stream_payloads() {
        // Arrange
        let expected = vec![crate::domains::stream::storage::encode_payload_blob_key(0)];
        let engine = seeded_engine(&expected);
        let tx = engine.begin_tx(1, TransactionMode::ReadOnly).unwrap();

        // Act
        let actual = scan_domain_rows(&tx, DomainKeyspace::Stream)
            .unwrap()
            .map(|row| row.unwrap().0.to_vec())
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(actual, expected);
    }

    #[test]
    fn should_preserve_unrecognized_rows_for_validation() {
        // Arrange
        let expected = vec![
            b"\0unknown\0row".to_vec(),
            b"\x05\0kv\0legacy".to_vec(),
            b"realm\0unknown\0row".to_vec(),
            b"\xea\0kv\0legacy".to_vec(),
        ];
        let engine = seeded_engine(&expected);
        let tx = engine.begin_tx(1, TransactionMode::ReadOnly).unwrap();

        // Act
        let actual = scan_domain_rows(&tx, DomainKeyspace::Stream)
            .unwrap()
            .map(|row| row.unwrap().0.to_vec())
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(actual, expected);
    }
}
