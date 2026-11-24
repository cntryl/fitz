use super::*;

#[test]
fn should_expose_scope_for_every_kv_message_variant() {
    // Arrange
    let scope = KvResourceScope::new(RouteFamily::new(7), "realm", "area", "resource");
    let key = Bytes::from_static(b"key");
    let value = Bytes::from_static(b"value");
    let messages = [
        KvMessage::Begin {
            scope: scope.clone(),
            mode: TxMode::ReadWrite,
        },
        KvMessage::Commit {
            tx_id: 1,
            scope: scope.clone(),
            persistence: CommitPersistence::Sync,
        },
        KvMessage::Rollback {
            tx_id: 1,
            scope: scope.clone(),
        },
        KvMessage::Get {
            tx_id: 1,
            scope: scope.clone(),
            key: key.clone(),
        },
        KvMessage::Put {
            tx_id: 1,
            scope: scope.clone(),
            key: key.clone(),
            value: value.clone(),
        },
        KvMessage::Insert {
            tx_id: 1,
            scope: scope.clone(),
            key: key.clone(),
            value,
        },
        KvMessage::Delete {
            tx_id: 1,
            scope: scope.clone(),
            key: key.clone(),
        },
        KvMessage::DeleteRange {
            tx_id: 1,
            scope: scope.clone(),
            start: key,
            end: Bytes::from_static(b"next"),
        },
        KvMessage::Scan {
            tx_id: 1,
            scope: scope.clone(),
            query: ScanQuery {
                start: None,
                end: None,
                limit: None,
                reverse: false,
                start_exclusive: false,
            },
        },
    ];

    // Act
    let actual = messages.iter().map(KvMessage::scope).collect::<Vec<_>>();

    // Assert
    assert_eq!(actual, vec![&scope; messages.len()]);
}
