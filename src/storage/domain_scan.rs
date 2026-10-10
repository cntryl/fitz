//! Family-scoped scans that avoid reading other domains' payload ranges.

use crate::utils::storage_key::{prefix_range_end, strip_domain_prefix, DomainKeyspace};
use bytes::Bytes;
use cntryl_midge::{MidgeResult, Query, Transaction};

/// Scan owned and unrecognized rows from one transaction snapshot.
///
/// Unrecognized and binary-leading rows remain visible for legacy/corruption
/// validation. Ordinary foreign realm/domain ranges cost one row and a seek.
pub(crate) fn scan_domain_rows(
    transaction: &Transaction,
    domain: DomainKeyspace,
) -> MidgeResult<impl Iterator<Item = MidgeResult<(Bytes, Bytes)>> + '_> {
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
        let Some(end) = foreign_domain_end(&key, domain) else {
            return Some(Ok((key, value)));
        };
        // The first foreign row identifies its realm/domain range. Seek past
        // the whole range instead of materializing every foreign payload.
        drop(value);
        rows = None;
        match transaction.scan(&Query::new().start_key(Bytes::from(end))) {
            Ok(next) => rows = Some(next),
            Err(error) => return Some(Err(error)),
        }
    }))
}

fn foreign_domain_end(key: &[u8], domain: DomainKeyspace) -> Option<Vec<u8>> {
    // Legacy Stream keys begin with a raw control or high-byte marker. Their
    // binary suffix can resemble a realm/domain prefix by coincidence. Keep
    // them visible; non-ASCII realm prefixes conservatively use the full scan.
    if !key.first()?.is_ascii_graphic() {
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
    fn should_preserve_unrecognized_and_legacy_rows_for_validation() {
        // Arrange
        let expected = vec![
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
