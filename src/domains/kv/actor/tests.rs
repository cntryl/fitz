use super::*;
pub(super) use crate::runtime::routing::RouteFamily;

pub(super) fn test_actor() -> KvActor {
    let store = crate::testkit::create_test_engine_with_cfs(vec![1, 2, 3]);
    KvActor::new(store)
}

mod conflict_and_error_paths;
mod inventory;
mod lifecycle;
mod range_and_pagination;
mod scope;
mod state_model;
mod wire_budget;
mod write_policy;

pub(super) fn begin_with_scope(actor: &mut KvActor, scope: KvResourceScope) -> u64 {
    let response = actor.handle(KvMessage::Begin {
        scope,
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let KvResponse::BeginOk { tx_id } = response else {
        panic!("expected transaction begin, got {response:?}");
    };
    tx_id
}

pub(super) fn put_scan_keys(
    actor: &mut KvActor,
    tx_id: u64,
    scope: &KvResourceScope,
    keys: &[&[u8]],
) {
    for key in keys {
        let response = actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(key),
        });
        assert!(matches!(response, KvResponse::PutOk));
    }
}

pub(super) fn scan_keys(
    actor: &mut KvActor,
    tx_id: u64,
    scope: &KvResourceScope,
    query: ScanQuery,
) -> (Vec<Bytes>, bool) {
    let response = actor.handle(KvMessage::Scan {
        tx_id,
        scope: scope.clone(),
        query,
    });
    let KvResponse::ScanResult { items, has_more } = response else {
        panic!("expected scan result, got {response:?}");
    };
    (items.into_iter().map(|item| item.key).collect(), has_more)
}
