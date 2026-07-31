use super::*;
use bytes::Bytes;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Begin,
    Put(u8, u8),
    Delete(u8),
    Get(u8),
    Commit,
    Rollback,
    StaleGet(u8),
    ScopeMismatch,
    Recreate,
}

fn operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        Just(Operation::Begin),
        (0_u8..8, any::<u8>()).prop_map(|(key, value)| Operation::Put(key, value)),
        (0_u8..8).prop_map(Operation::Delete),
        (0_u8..8).prop_map(Operation::Get),
        Just(Operation::Commit),
        Just(Operation::Rollback),
        (0_u8..8).prop_map(Operation::StaleGet),
        Just(Operation::ScopeMismatch),
        Just(Operation::Recreate),
    ]
}

fn scope() -> KvResourceScope {
    KvResourceScope::new(RouteFamily::new(1), "model", "kv", "records")
}

fn other_scope() -> KvResourceScope {
    KvResourceScope::new(RouteFamily::new(1), "model", "kv", "other")
}

fn begin(actor: &mut KvActor) -> u64 {
    let KvResponse::BeginOk { tx_id } = actor.handle(KvMessage::Begin {
        scope: scope(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    }) else {
        panic!("state model begin must succeed");
    };
    tx_id
}

fn read_committed(actor: &mut KvActor) -> BTreeMap<u8, u8> {
    let tx_id = begin(actor);
    let mut values = BTreeMap::new();
    for key in 0_u8..8 {
        if let KvResponse::GetResult {
            found: true,
            value: Some(value),
        } = actor.handle(KvMessage::Get {
            tx_id,
            scope: scope(),
            key: Bytes::from(vec![key]),
        }) {
            values.insert(key, value[0]);
        }
    }
    assert!(matches!(
        actor.handle(KvMessage::Rollback {
            tx_id,
            scope: scope()
        }),
        KvResponse::RollbackOk
    ));
    values
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        max_shrink_iters: 10_000,
        ..ProptestConfig::default()
    })]

    #[test]
    fn should_match_kv_reference_state_model(operations in prop::collection::vec(operation(), 1..=64)) {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let mut actor = KvActor::new(Arc::clone(&store));
        let mut committed = BTreeMap::<u8, u8>::new();
        let mut staged: Option<BTreeMap<u8, u8>> = None;
        let mut active_tx = None;
        let stale_tx = u64::MAX;

        // Act
        for operation in operations {
            match operation {
                Operation::Begin if active_tx.is_none() => {
                    let tx_id = begin(&mut actor);
                    active_tx = Some(tx_id);
                    staged = Some(committed.clone());
                }
                Operation::Begin => {
                    let tx_id = begin(&mut actor);
                    let response = actor.handle(KvMessage::Rollback {
                        tx_id,
                        scope: scope(),
                    });
                    prop_assert!(
                        matches!(response, KvResponse::RollbackOk),
                        "parallel transaction cleanup must succeed"
                    );
                }
                Operation::Put(key, value) => {
                    let tx_id = active_tx.unwrap_or(stale_tx);
                    let response = actor.handle(KvMessage::Put {
                        tx_id,
                        scope: scope(),
                        key: Bytes::from(vec![key]),
                        value: Bytes::from(vec![value]),
                    });
                    if let Some(model) = staged.as_mut() {
                        prop_assert!(matches!(response, KvResponse::PutOk));
                        model.insert(key, value);
                    } else {
                        prop_assert!(matches!(response, KvResponse::Error { error: KvError::InvalidTxId }), "expected invalid transaction");
                    }
                }
                Operation::Delete(key) => {
                    let tx_id = active_tx.unwrap_or(stale_tx);
                    let response = actor.handle(KvMessage::Delete {
                        tx_id,
                        scope: scope(),
                        key: Bytes::from(vec![key]),
                    });
                    if let Some(model) = staged.as_mut() {
                        prop_assert!(
                            matches!(response, KvResponse::DeleteOk),
                            "expected delete response"
                        );
                        model.remove(&key);
                    } else {
                        prop_assert!(matches!(response, KvResponse::Error { error: KvError::InvalidTxId }), "expected invalid transaction");
                    }
                }
                Operation::Get(key) => {
                    let tx_id = active_tx.unwrap_or(stale_tx);
                    let response = actor.handle(KvMessage::Get {
                        tx_id,
                        scope: scope(),
                        key: Bytes::from(vec![key]),
                    });
                    if let Some(model) = staged.as_ref() {
                        match (model.get(&key), response) {
                            (Some(expected), KvResponse::GetResult { found: true, value: Some(actual) }) => {
                                prop_assert_eq!(actual.as_ref(), &[*expected]);
                            }
                            (None, KvResponse::GetResult { found: false, value: None }) => {}
                            pair => prop_assert!(false, "unexpected model/actor read: {pair:?}"),
                        }
                    } else {
                        prop_assert!(matches!(response, KvResponse::Error { error: KvError::InvalidTxId }), "expected invalid transaction");
                    }
                }
                Operation::Commit if active_tx.is_some() => {
                    let tx_id = active_tx.take().expect("checked active transaction");
                    let response = actor.handle(KvMessage::Commit { tx_id, scope: scope() });
                    prop_assert!(
                        matches!(response, KvResponse::CommitOk),
                        "expected commit response"
                    );
                    committed = staged.take().expect("active model transaction");
                }
                Operation::Rollback if active_tx.is_some() => {
                    let tx_id = active_tx.take().expect("checked active transaction");
                    let response = actor.handle(KvMessage::Rollback { tx_id, scope: scope() });
                    prop_assert!(matches!(response, KvResponse::RollbackOk));
                    staged = None;
                }
                Operation::StaleGet(key) => {
                    let response = actor.handle(KvMessage::Get {
                        tx_id: stale_tx,
                        scope: scope(),
                        key: Bytes::from(vec![key]),
                    });
                    prop_assert!(matches!(response, KvResponse::Error { error: KvError::InvalidTxId }), "expected invalid transaction");
                }
                Operation::ScopeMismatch if active_tx.is_some() => {
                    let response = actor.handle(KvMessage::Get {
                        tx_id: active_tx.expect("checked active transaction"),
                        scope: other_scope(),
                        key: Bytes::from_static(b"x"),
                    });
                    prop_assert!(matches!(response, KvResponse::Error { error: KvError::TxScopeViolation { .. } }), "expected scope violation");
                }
                Operation::Commit | Operation::Rollback | Operation::ScopeMismatch => {}
                Operation::Recreate => {
                    active_tx = None;
                    staged = None;
                    actor = KvActor::new(Arc::clone(&store));
                }
            }

            // Assert
            if active_tx.is_none() {
                prop_assert_eq!(read_committed(&mut actor), committed.clone());
            }
            prop_assert_eq!(actor.transaction_count(), usize::from(active_tx.is_some()));
        }
    }
}
