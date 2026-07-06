use super::common::*;

#[test]
fn should_accept_single_chunk_response() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();
    let response = RpcResponseMsg::single(correlation_id, Bytes::from(vec![1, 2, 3]));

    // Act
    inbox.receive(InboxMessage::Response(response), &mut ctx);

    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_handle_in_order_streaming_chunks() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 1, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 2, true)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_fail_out_of_order_chunks() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 2, false)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 0);
    assert_eq!(inbox.buffered_count(&correlation_id), 0);
    assert_eq!(inbox.invalid_sequence_failures(), 1);
}

#[test]
fn should_fail_when_gap_appears_mid_stream() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 2, false)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 0);
    assert_eq!(inbox.buffered_count(&correlation_id), 0);
    assert_eq!(inbox.invalid_sequence_failures(), 1);
}

#[test]
fn should_cleanup_stream_when_final_chunk_received() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    assert_eq!(inbox.active_streams(), 1);

    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 1, true)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_fail_when_final_chunk_arrives_before_gap_is_closed() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 2, true)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 0);
    assert_eq!(inbox.buffered_count(&correlation_id), 0);
    assert_eq!(inbox.invalid_sequence_failures(), 1);
}

#[test]
fn should_drop_duplicate_chunks() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 0);
    assert_eq!(inbox.invalid_sequence_failures(), 1);
}

#[test]
fn should_handle_multiple_concurrent_streams() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id_1 = Uuid::new_v4();
    let correlation_id_2 = Uuid::new_v4();
    let correlation_id_3 = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id_1, 0, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id_2, 0, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id_3, 0, false)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 3);
}

#[test]
fn should_isolate_streams_by_correlation_id() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id_1 = Uuid::new_v4();
    let correlation_id_2 = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id_1, 0, true)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id_2, 0, false)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 1);
    assert_eq!(inbox.buffered_count(&correlation_id_1), 0);
    assert_eq!(inbox.buffered_count(&correlation_id_2), 0);
}

#[test]
fn should_preserve_strict_sequence_contract_with_custom_buffer_size() {
    // Arrange
    let mut inbox = ReplyInboxActor::with_buffer_size(RouteFamily::new(1), 5);
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 5, false)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 0);
    assert_eq!(inbox.invalid_sequence_failures(), 1);
}

#[test]
fn should_cleanup_stream_on_explicit_cleanup_message() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    assert_eq!(inbox.active_streams(), 1);

    // Act
    inbox.receive(InboxMessage::Cleanup { correlation_id }, &mut ctx);

    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_fail_large_sequence_gaps() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 100, false)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 0);
    assert_eq!(inbox.buffered_count(&correlation_id), 0);
    assert_eq!(inbox.invalid_sequence_failures(), 1);
}
