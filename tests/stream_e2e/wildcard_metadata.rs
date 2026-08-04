use super::common::*;

pub(crate) async fn should_read_committed_area_history_given_wildcard_route<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let orders_route = "stream://test/events/orders";
    let audits_route = "stream://test/events/audits";

    // Act
    commit_stream_record(&mut client, orders_route, b"order-created").await;
    commit_stream_record_with_offset(&mut client, audits_route, 0, b"audit-recorded").await;
    commit_stream_record_with_offset(&mut client, orders_route, 1, b"order-shipped").await;

    let first_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/events/*", 0, 2, None),
            2000,
        )
        .await
        .expect("read area history");
    let second_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/events/*", 2, 2, None),
            2000,
        )
        .await
        .expect("resume area history");

    // Assert
    let first = parse_stream_read_response(&first_response);
    let second = parse_stream_read_response(&second_response);
    let bodies: Vec<Vec<u8>> = first
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    let area_offsets: Vec<Option<u64>> = first
        .records
        .iter()
        .map(|record| record.area_offset)
        .collect();
    assert_eq!(
        bodies,
        vec![b"order-created".to_vec(), b"audit-recorded".to_vec()]
    );
    assert_eq!(first.routes, vec![orders_route, audits_route]);
    assert_eq!(area_offsets, vec![Some(0), Some(1)]);
    assert_eq!(first.cursor.last_area_offset, Some(1));
    assert!(first.cursor.has_more);

    assert_eq!(second.records.len(), 1);
    assert_eq!(second.routes, vec![orders_route]);
    assert_eq!(second.records[0].body, b"order-shipped".to_vec());
    assert_eq!(second.records[0].area_offset, Some(2));
    assert_eq!(second.cursor.last_area_offset, Some(2));
    assert!(!second.cursor.has_more);
}

pub(crate) async fn should_read_committed_realm_history_given_wildcard_route<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let events_route = "stream://test/events/orders";
    let audit_route = "stream://test/audit/ledger";

    // Act
    commit_stream_record(&mut client, events_route, b"realm-one").await;
    commit_stream_record_with_offset(&mut client, audit_route, 0, b"realm-two").await;
    commit_stream_record_with_offset(&mut client, events_route, 1, b"realm-three").await;

    let first_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/*/*", 0, 2, None),
            2000,
        )
        .await
        .expect("read realm history");
    let second_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/*/*", 2, 2, None),
            2000,
        )
        .await
        .expect("resume realm history");

    // Assert
    let first = parse_stream_read_response(&first_response);
    let second = parse_stream_read_response(&second_response);
    let bodies: Vec<Vec<u8>> = first
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    let realm_offsets: Vec<Option<u64>> = first
        .records
        .iter()
        .map(|record| record.realm_offset)
        .collect();
    assert_eq!(bodies, vec![b"realm-one".to_vec(), b"realm-two".to_vec()]);
    assert_eq!(first.routes, vec![events_route, audit_route]);
    assert_eq!(realm_offsets, vec![Some(0), Some(1)]);
    assert_eq!(first.cursor.last_realm_offset, Some(1));
    assert!(first.cursor.has_more);

    assert_eq!(second.records.len(), 1);
    assert_eq!(second.routes, vec![events_route]);
    assert_eq!(second.records[0].body, b"realm-three".to_vec());
    assert_eq!(second.records[0].realm_offset, Some(2));
    assert_eq!(second.cursor.last_realm_offset, Some(2));
    assert!(!second.cursor.has_more);
}

pub(crate) async fn should_stop_area_wildcard_read_given_max_bytes<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let orders_route = "stream://test/events/orders";
    let audits_route = "stream://test/events/audits";

    commit_stream_record(&mut client, orders_route, b"abcd").await;
    commit_stream_record_with_offset(&mut client, audits_route, 0, b"efgh").await;

    // Act
    let first_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/events/*", 0, 10, Some(4)),
            2000,
        )
        .await
        .expect("read first area wildcard page");
    let second_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/events/*", 1, 10, Some(4)),
            2000,
        )
        .await
        .expect("read second area wildcard page");

    // Assert
    let first = parse_stream_read_response(&first_response);
    let second = parse_stream_read_response(&second_response);
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.records[0].body, b"abcd".to_vec());
    assert_eq!(first.records[0].area_offset, Some(0));
    assert_eq!(first.cursor.last_area_offset, Some(0));
    assert!(first.cursor.has_more);

    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].body, b"efgh".to_vec());
    assert_eq!(second.records[0].area_offset, Some(1));
    assert_eq!(second.cursor.last_area_offset, Some(1));
    assert!(!second.cursor.has_more);
}

pub(crate) async fn should_stop_realm_wildcard_read_given_max_bytes<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let events_route = "stream://test/events/orders";
    let audit_route = "stream://test/audit/ledger";

    commit_stream_record(&mut client, events_route, b"abcd").await;
    commit_stream_record_with_offset(&mut client, audit_route, 0, b"efgh").await;

    // Act
    let first_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/*/*", 0, 10, Some(4)),
            2000,
        )
        .await
        .expect("read first realm wildcard page");
    let second_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/*/*", 1, 10, Some(4)),
            2000,
        )
        .await
        .expect("read second realm wildcard page");

    // Assert
    let first = parse_stream_read_response(&first_response);
    let second = parse_stream_read_response(&second_response);
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.records[0].body, b"abcd".to_vec());
    assert_eq!(first.records[0].realm_offset, Some(0));
    assert_eq!(first.cursor.last_realm_offset, Some(0));
    assert!(first.cursor.has_more);

    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].body, b"efgh".to_vec());
    assert_eq!(second.records[0].realm_offset, Some(1));
    assert_eq!(second.cursor.last_realm_offset, Some(1));
    assert!(!second.cursor.has_more);
}

pub(crate) async fn should_expose_exact_resource_record_metadata_on_read<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/rich";
    let begin_response = client
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin stream session");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client
        .send_and_receive(
            &build_stream_append_with_metadata(session_id, 0, b"payload", Some(b"meta")),
            2000,
        )
        .await
        .expect("append rich stream event");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Expected success for stream append");

    let commit_response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("commit rich stream session");
    let (_msg_type, status, _data) = parse_stream_response(&commit_response);
    assert_eq!(status, 0, "Expected success for stream commit");

    // Act
    let read_response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read rich stream event");
    let read = parse_stream_read_response(&read_response);

    // Assert
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].resource_offset, 0);
    assert_eq!(read.records[0].area_offset, Some(0));
    assert_eq!(read.records[0].realm_offset, Some(0));
    assert_eq!(read.records[0].body, b"payload".to_vec());
    assert_eq!(read.records[0].metadata, Some(b"meta".to_vec()));
    assert!(read.records[0].created_at > 0);
    assert_eq!(read.cursor.last_resource_offset, 0);
    assert_eq!(read.cursor.last_area_offset, Some(0));
    assert_eq!(read.cursor.last_realm_offset, Some(0));
    assert!(!read.cursor.has_more);
}

pub(crate) async fn should_expose_exact_resource_record_metadata_on_last<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/last-rich";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let last_response = client
        .send_and_receive(&build_stream_last(route), 2000)
        .await
        .expect("read stream tail");
    let record = parse_stream_last_response(&last_response).expect("expected tail record");

    // Assert
    assert_eq!(record.resource_offset, 0);
    assert_eq!(record.area_offset, Some(0));
    assert_eq!(record.realm_offset, Some(0));
    assert_eq!(record.body, b"payload".to_vec());
    assert_eq!(record.metadata, None);
    assert!(record.created_at > 0);
}

pub(crate) async fn should_expose_exact_resource_metadata_on_get_metadata<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/meta";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let metadata_response = client
        .send_and_receive(&build_stream_get_metadata(route), 2000)
        .await
        .expect("read stream metadata");
    let metadata = parse_stream_metadata_response(&metadata_response);

    // Assert
    assert_eq!(metadata.first_resource_offset, Some(0));
    assert_eq!(metadata.last_resource_offset, Some(0));
    assert_eq!(metadata.resource_count, 1);
    assert_eq!(metadata.max_batch_events, 10_000);
    assert_eq!(metadata.max_batch_bytes, 10 * 1024 * 1024);
    assert_eq!(metadata.ttl_seconds, None);
    assert_eq!(metadata.area_watermark, 0);
    assert_eq!(metadata.realm_watermark, 0);
}

pub(crate) async fn should_stop_exact_resource_read_given_max_bytes<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/max-bytes";
    commit_stream_record_with_offset(&mut client, route, 0, b"abcd").await;
    commit_stream_record_with_offset(&mut client, route, 1, b"efgh").await;

    // Act
    let first_response = client
        .send_and_receive(&build_stream_read_with_options(route, 0, 10, Some(4)), 2000)
        .await
        .expect("read first byte-limited page");
    let second_response = client
        .send_and_receive(&build_stream_read_with_options(route, 1, 10, Some(4)), 2000)
        .await
        .expect("read resumed byte-limited page");

    // Assert
    let first = parse_stream_read_response(&first_response);
    let second = parse_stream_read_response(&second_response);
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.records[0].body, b"abcd".to_vec());
    assert_eq!(first.cursor.last_resource_offset, 0);
    assert!(first.cursor.has_more);
    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].body, b"efgh".to_vec());
    assert_eq!(second.cursor.last_resource_offset, 1);
    assert!(!second.cursor.has_more);
}

pub(crate) async fn should_return_empty_success_given_area_wildcard_last<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/orders";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let response = client
        .send_and_receive(&build_stream_last("stream://test/events/*"), 2000)
        .await
        .expect("read area wildcard last");

    // Assert
    assert_eq!(parse_stream_ok_data(&response), Vec::<u8>::new());
    assert!(parse_stream_last_response(&response).is_none());
}

pub(crate) async fn should_return_empty_success_given_realm_wildcard_last<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/orders";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let response = client
        .send_and_receive(&build_stream_last("stream://test/*/*"), 2000)
        .await
        .expect("read realm wildcard last");

    // Assert
    assert_eq!(parse_stream_ok_data(&response), Vec::<u8>::new());
    assert!(parse_stream_last_response(&response).is_none());
}

pub(crate) async fn should_return_empty_success_given_area_wildcard_get_metadata<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/orders";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let response = client
        .send_and_receive(&build_stream_get_metadata("stream://test/events/*"), 2000)
        .await
        .expect("read area wildcard metadata");

    // Assert
    assert_eq!(parse_stream_ok_data(&response), Vec::<u8>::new());
}

pub(crate) async fn should_return_empty_success_given_realm_wildcard_get_metadata<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/orders";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let response = client
        .send_and_receive(&build_stream_get_metadata("stream://test/*/*"), 2000)
        .await
        .expect("read realm wildcard metadata");

    // Assert
    assert_eq!(parse_stream_ok_data(&response), Vec::<u8>::new());
}
