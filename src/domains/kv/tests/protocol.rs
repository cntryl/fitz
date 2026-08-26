use super::*;

#[test]
fn should_expose_scope_for_every_kv_message_variant() {
    // Arrange
    let scope = KvResourceScope::new(RouteFamily::new(7), "realm", "area", "resource");
    let message = KvMessage::Rollback {
        tx_id: 1,
        scope: scope.clone(),
    };

    // Act
    let actual = message.scope();

    // Assert
    assert_eq!(actual, &scope);
}
