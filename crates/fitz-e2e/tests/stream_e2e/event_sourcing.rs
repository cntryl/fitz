use super::common::*;

pub(crate) async fn should_support_event_sourced_aggregate_command_and_rebuild<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://event-sourced/orders/order-123";
    let opened = br#"{"type":"OrderOpened","orderId":"order-123"}"#;
    let line_added = br#"{"type":"LineAdded","sku":"sku-1","qty":2}"#;
    let confirmed = br#"{"type":"OrderConfirmed"}"#;

    // Act
    let command_session_id = begin_stream_session(&mut client, route).await;
    append_stream_record_with_metadata(
        &mut client,
        command_session_id,
        0,
        opened,
        Some(b"type=OrderOpened;command=cmd-001"),
    )
    .await;
    append_stream_record_with_metadata(
        &mut client,
        command_session_id,
        1,
        line_added,
        Some(b"type=LineAdded;command=cmd-001"),
    )
    .await;
    commit_stream_session(&mut client, command_session_id).await;

    let confirm_session_id = begin_stream_session(&mut client, route).await;
    append_stream_record_with_metadata(
        &mut client,
        confirm_session_id,
        2,
        confirmed,
        Some(b"type=OrderConfirmed;command=cmd-002"),
    )
    .await;
    commit_stream_session(&mut client, confirm_session_id).await;

    let read_response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("rebuild aggregate from stream");
    let last_response = client
        .send_and_receive(&build_stream_last(route), 2000)
        .await
        .expect("read current aggregate revision");
    let metadata_response = client
        .send_and_receive(&build_stream_get_metadata(route), 2000)
        .await
        .expect("read aggregate stream metadata");

    // Assert
    let read = parse_stream_read_response(&read_response);
    let resource_offsets: Vec<u64> = read
        .records
        .iter()
        .map(|record| record.resource_offset)
        .collect();
    let area_offsets: Vec<Option<u64>> = read
        .records
        .iter()
        .map(|record| record.area_offset)
        .collect();
    let realm_offsets: Vec<Option<u64>> = read
        .records
        .iter()
        .map(|record| record.realm_offset)
        .collect();
    let bodies: Vec<Vec<u8>> = read
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    let metadata: Vec<Option<Vec<u8>>> = read
        .records
        .iter()
        .map(|record| record.metadata.clone())
        .collect();
    assert_eq!(resource_offsets, vec![0, 1, 2]);
    assert_eq!(area_offsets, vec![Some(0), Some(1), Some(2)]);
    assert_eq!(realm_offsets, vec![Some(0), Some(1), Some(2)]);
    assert_eq!(
        bodies,
        vec![opened.to_vec(), line_added.to_vec(), confirmed.to_vec()]
    );
    assert_eq!(
        metadata,
        vec![
            Some(b"type=OrderOpened;command=cmd-001".to_vec()),
            Some(b"type=LineAdded;command=cmd-001".to_vec()),
            Some(b"type=OrderConfirmed;command=cmd-002".to_vec()),
        ]
    );

    let last = parse_stream_last_response(&last_response).expect("current aggregate event");
    assert_eq!(last.resource_offset, 2);
    assert_eq!(last.body, confirmed.to_vec());

    let metadata = parse_stream_metadata_response(&metadata_response);
    assert_eq!(metadata.first_resource_offset, Some(0));
    assert_eq!(metadata.last_resource_offset, Some(2));
    assert_eq!(metadata.resource_count, 3);
}

pub(crate) async fn should_leave_event_sourced_history_unchanged_given_stale_revision<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://event-sourced/orders/order-456";
    let opened = br#"{"type":"OrderOpened","orderId":"order-456"}"#;
    let renamed = br#"{"type":"OrderRenamed","name":"priority"}"#;
    commit_stream_record(&mut client, route, opened).await;

    // Act
    let stale_session_id = begin_stream_session(&mut client, route).await;
    let stale_append_response = client
        .send_and_receive(&build_stream_append(stale_session_id, 0, renamed), 2000)
        .await
        .expect("append stale aggregate revision");
    let stale_error = parse_stream_error_message(&stale_append_response);

    let read_after_conflict_response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read aggregate after conflict");
    let last_after_conflict_response = client
        .send_and_receive(&build_stream_last(route), 2000)
        .await
        .expect("read aggregate tail after conflict");
    let metadata_after_conflict_response = client
        .send_and_receive(&build_stream_get_metadata(route), 2000)
        .await
        .expect("read aggregate metadata after conflict");
    rollback_stream_session(&mut client, stale_session_id).await;

    let retry_session_id = begin_stream_session(&mut client, route).await;
    append_stream_record_with_metadata(
        &mut client,
        retry_session_id,
        1,
        renamed,
        Some(b"type=OrderRenamed;command=cmd-002"),
    )
    .await;
    commit_stream_session(&mut client, retry_session_id).await;

    let final_read_response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read aggregate after retry");

    // Assert
    assert!(stale_error.contains("concurrency conflict"));
    let read_after_conflict = parse_stream_read_response(&read_after_conflict_response);
    assert_eq!(read_after_conflict.records.len(), 1);
    assert_eq!(read_after_conflict.records[0].resource_offset, 0);
    assert_eq!(read_after_conflict.records[0].body, opened.to_vec());

    let last_after_conflict =
        parse_stream_last_response(&last_after_conflict_response).expect("aggregate tail");
    assert_eq!(last_after_conflict.resource_offset, 0);
    assert_eq!(last_after_conflict.body, opened.to_vec());

    let metadata_after_conflict = parse_stream_metadata_response(&metadata_after_conflict_response);
    assert_eq!(metadata_after_conflict.first_resource_offset, Some(0));
    assert_eq!(metadata_after_conflict.last_resource_offset, Some(0));
    assert_eq!(metadata_after_conflict.resource_count, 1);

    let final_read = parse_stream_read_response(&final_read_response);
    let final_bodies: Vec<Vec<u8>> = final_read
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    let final_offsets: Vec<u64> = final_read
        .records
        .iter()
        .map(|record| record.resource_offset)
        .collect();
    assert_eq!(final_offsets, vec![0, 1]);
    assert_eq!(final_bodies, vec![opened.to_vec(), renamed.to_vec()]);
}

pub(crate) async fn should_support_general_stream_replay_across_resource_area_and_realm<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let sensor_a = "stream://general/telemetry/sensor-a";
    let sensor_b = "stream://general/telemetry/sensor-b";
    let ledger = "stream://general/audit/ledger";

    // Act
    commit_stream_record(&mut client, sensor_a, b"sensor-a:0").await;
    commit_stream_record(&mut client, sensor_b, b"sensor-b:0").await;
    commit_stream_record(&mut client, ledger, b"ledger:0").await;
    commit_stream_record_with_offset(&mut client, sensor_a, 1, b"sensor-a:1").await;

    let exact_response = client
        .send_and_receive(&build_stream_read_with_options(sensor_a, 0, 10, None), 2000)
        .await
        .expect("read exact stream resource");
    let area_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://general/telemetry/*", 0, 10, None),
            2000,
        )
        .await
        .expect("read area stream");
    let realm_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://general/*/*", 0, 10, None),
            2000,
        )
        .await
        .expect("read realm stream");

    // Assert
    let exact = parse_stream_read_response(&exact_response);
    let exact_offsets: Vec<u64> = exact
        .records
        .iter()
        .map(|record| record.resource_offset)
        .collect();
    let exact_bodies: Vec<Vec<u8>> = exact
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    assert_eq!(exact_offsets, vec![0, 1]);
    assert_eq!(
        exact_bodies,
        vec![b"sensor-a:0".to_vec(), b"sensor-a:1".to_vec()]
    );

    let area = parse_stream_read_response(&area_response);
    let area_bodies: Vec<Vec<u8>> = area
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    let area_offsets: Vec<Option<u64>> = area
        .records
        .iter()
        .map(|record| record.area_offset)
        .collect();
    assert_eq!(
        area_bodies,
        vec![
            b"sensor-a:0".to_vec(),
            b"sensor-b:0".to_vec(),
            b"sensor-a:1".to_vec(),
        ]
    );
    assert_eq!(area_offsets, vec![Some(0), Some(1), Some(2)]);

    let realm = parse_stream_read_response(&realm_response);
    let realm_bodies: Vec<Vec<u8>> = realm
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    let realm_offsets: Vec<Option<u64>> = realm
        .records
        .iter()
        .map(|record| record.realm_offset)
        .collect();
    assert_eq!(
        realm_bodies,
        vec![
            b"sensor-a:0".to_vec(),
            b"sensor-b:0".to_vec(),
            b"ledger:0".to_vec(),
            b"sensor-a:1".to_vec(),
        ]
    );
    assert_eq!(realm_offsets, vec![Some(0), Some(1), Some(2), Some(3)]);
}
