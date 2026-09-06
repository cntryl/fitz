use super::common::*;

// Generic test helper for appending to stream
pub(crate) async fn should_append_data_to_stream<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/audit";

    // Act
    commit_stream_record(&mut client, route, b"event-001").await;
}

// Generic test helper for reading from stream
pub(crate) async fn should_read_appended_data<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/logs/main";
    let test_data = b"stream-record-1";
    commit_stream_record(&mut client, route, test_data).await;

    // Act
    let read_frame = build_stream_read(route, 0);
    let response = client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("read");

    // Assert
    let read = parse_stream_read_response(&response);
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].resource_offset, 0);
    assert_eq!(read.records[0].body, test_data.to_vec());
    assert_eq!(read.cursor.last_resource_offset, 0);
    assert_eq!(read.cursor.last_area_offset, Some(0));
    assert_eq!(read.cursor.last_realm_offset, Some(0));
    assert!(!read.cursor.has_more);
}

// Generic test helper for read ordering
pub(crate) async fn should_preserve_append_order<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/ordered/main";

    // Act
    commit_stream_record_with_offset(&mut client, route, 0, b"first").await;
    commit_stream_record_with_offset(&mut client, route, 1, b"second").await;

    let read_frame = build_stream_read(route, 0);
    let response = client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("read");

    // Assert
    let read = parse_stream_read_response(&response);
    let offsets: Vec<u64> = read
        .records
        .iter()
        .map(|record| record.resource_offset)
        .collect();
    let bodies: Vec<Vec<u8>> = read
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    assert_eq!(offsets, vec![0, 1]);
    assert_eq!(bodies, vec![b"first".to_vec(), b"second".to_vec()]);
    assert_eq!(read.cursor.last_resource_offset, 1);
    assert_eq!(read.cursor.last_area_offset, Some(1));
    assert_eq!(read.cursor.last_realm_offset, Some(1));
    assert!(!read.cursor.has_more);
}

// Generic test helper for read past end
pub(crate) async fn should_handle_read_past_end<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/sparse/main";
    commit_stream_record(&mut client, route, b"present").await;
    let frame = build_stream_read(route, 1);

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let read = parse_stream_read_response(&response);
    assert!(read.records.is_empty());
    assert_eq!(read.cursor.last_resource_offset, 1);
    assert_eq!(read.cursor.last_area_offset, None);
    assert_eq!(read.cursor.last_realm_offset, None);
    assert!(!read.cursor.has_more);
}

// Generic test helper for FIFO ordering with multiple appends
pub(crate) async fn should_maintain_fifo_order_with_multiple_appends<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/fifo/main";

    // Act - Append 5 events
    for i in 1..=5 {
        let data = format!("event-{i}").into_bytes();
        commit_stream_record_with_offset(&mut client, route, i32_to_u64_nonnegative(i - 1), &data)
            .await;
    }

    // Act
    let response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read stream history");

    // Assert
    let read = parse_stream_read_response(&response);
    let offsets: Vec<u64> = read
        .records
        .iter()
        .map(|record| record.resource_offset)
        .collect();
    let bodies: Vec<Vec<u8>> = read
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    assert_eq!(offsets, vec![0, 1, 2, 3, 4]);
    assert_eq!(
        bodies,
        vec![
            b"event-1".to_vec(),
            b"event-2".to_vec(),
            b"event-3".to_vec(),
            b"event-4".to_vec(),
            b"event-5".to_vec(),
        ]
    );
    assert_eq!(read.cursor.last_resource_offset, 4);
    assert!(!read.cursor.has_more);
}

// Generic test helper for large stream payloads
pub(crate) async fn should_handle_large_stream_payload<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/large/main";
    let large_data = vec![b'D'; 60_000]; // Within u16 TLV length limit (65535)

    // Act
    commit_stream_record(&mut client, route, &large_data).await;
}

// Generic test helper for concurrent appends from multiple clients
pub(crate) async fn should_handle_concurrent_appends_from_multiple_clients<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client1 = C::connect(server).await.expect("connect 1");
    let mut client2 = C::connect(server).await.expect("connect 2");
    let route = "stream://test/concurrent/main";

    // Act
    commit_stream_record_with_offset(&mut client1, route, 0, b"client-1-event").await;

    let begin_response = client2
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin stale stream write");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client2
        .send_and_receive(&build_stream_append(session_id, 0, b"client-2-event"), 2000)
        .await
        .expect("append stale stream write");
    let (code, _message) = parse_stream_error(&append_response);

    let read_response = client1
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read committed stream history");

    // Assert
    assert_eq!(code, 2001);
    let read = parse_stream_read_response(&read_response);
    let bodies: Vec<Vec<u8>> = read.records.into_iter().map(|record| record.body).collect();
    assert_eq!(bodies, vec![b"client-1-event".to_vec()]);
}

pub(crate) async fn should_reject_future_expected_offset_given_gap<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/concurrent/future-offset";
    commit_stream_record(&mut client, route, b"event-0").await;

    // Act
    let begin_response = client
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin future-offset stream write");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client
        .send_and_receive(&build_stream_append(session_id, 2, b"gap"), 2000)
        .await
        .expect("append future-offset stream write");
    let (code, _message) = parse_stream_error(&append_response);

    // Assert
    assert_eq!(code, 2001);
}

pub(crate) async fn should_return_session_not_found_given_append_to_unknown_session<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act
    let response = client
        .send_and_receive(&build_stream_append(999, 0, b"ghost"), 2000)
        .await
        .expect("append unknown stream session");
    let error = parse_stream_error_message(&response);

    // Assert
    assert!(error.contains("session not found"));
}

pub(crate) async fn should_return_session_not_found_given_commit_to_unknown_session<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act
    let response = client
        .send_and_receive(&build_stream_commit(999), 2000)
        .await
        .expect("commit unknown stream session");
    let error = parse_stream_error_message(&response);

    // Assert
    assert!(error.contains("session not found"));
}

pub(crate) async fn should_return_session_not_found_given_rollback_to_unknown_session<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act
    let response = client
        .send_and_receive(&build_stream_rollback(999), 2000)
        .await
        .expect("rollback unknown stream session");
    let error = parse_stream_error_message(&response);

    // Assert
    assert!(error.contains("session not found"));
}

pub(crate) async fn should_return_error_given_empty_batch_commit<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/errors/empty-batch";
    let begin_response = client
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin empty-batch stream session");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    // Act
    let response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("commit empty stream session");
    let error = parse_stream_error_message(&response);

    // Assert
    assert!(error.contains("empty batch"));
}

pub(crate) async fn should_return_empty_success_given_zero_limit_read<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/limits/zero-read";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let response = client
        .send_and_receive(&build_stream_read_with_options(route, 0, 0, None), 2000)
        .await
        .expect("read zero-limit stream history");
    let read = parse_stream_read_response(&response);

    // Assert
    assert!(read.records.is_empty());
    assert_eq!(read.cursor.last_resource_offset, 0);
    assert_eq!(read.cursor.last_area_offset, None);
    assert_eq!(read.cursor.last_realm_offset, None);
    assert!(!read.cursor.has_more);
}

pub(crate) async fn should_keep_connection_open_given_malformed_stream_filter_read<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/filters/malformed";
    commit_stream_record(&mut client, route, b"payload").await;
    let malformed_filter = [0, 0xF2, 0, 0, 0, 0];

    // Act
    let malformed_response = client
        .send_and_receive(
            &build_stream_read_with_raw_filter(route, 0, 10, None, &malformed_filter),
            2000,
        )
        .await
        .expect("read malformed stream filter payload");
    let malformed_error = parse_stream_error_message(&malformed_response);

    let valid_read_response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read valid stream history after malformed filter");
    let valid_read = parse_stream_read_response(&valid_read_response);

    // Assert
    assert!(malformed_error.contains("ERR_STREAM_FILTER_UNSUPPORTED_VERSION"));
    assert_eq!(valid_read.records.len(), 1);
    assert_eq!(valid_read.records[0].body, b"payload".to_vec());
}

pub(crate) async fn should_keep_connection_open_given_invalid_stream_filter_payload<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/filters/invalid-payload";
    commit_stream_record(&mut client, route, b"payload").await;
    let invalid_filter = [0, 0xF1, 0, 0, 0, 1, 9];

    // Act
    let invalid_response = client
        .send_and_receive(
            &build_stream_read_with_raw_filter(route, 0, 10, None, &invalid_filter),
            2000,
        )
        .await
        .expect("read invalid stream filter payload");
    let invalid_error = parse_stream_error_message(&invalid_response);

    let valid_read_response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read valid stream history after invalid filter payload");
    let valid_read = parse_stream_read_response(&valid_read_response);

    // Assert
    assert!(invalid_error.contains("ERR_STREAM_FILTER_INVALID_PAYLOAD"));
    assert_eq!(valid_read.records.len(), 1);
    assert_eq!(valid_read.records[0].body, b"payload".to_vec());
}

pub(crate) async fn should_return_none_given_empty_resource_last<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/empty/last";

    // Act
    let response = client
        .send_and_receive(&build_stream_last(route), 2000)
        .await
        .expect("read empty resource tail");

    // Assert
    assert_eq!(parse_stream_ok_data(&response), Vec::<u8>::new());
    assert!(parse_stream_last_response(&response).is_none());
}

pub(crate) async fn should_return_empty_metadata_given_empty_resource<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/empty/meta";

    // Act
    let response = client
        .send_and_receive(&build_stream_get_metadata(route), 2000)
        .await
        .expect("read empty resource metadata");
    let metadata = parse_stream_metadata_response(&response);

    // Assert
    assert_eq!(metadata.first_resource_offset, None);
    assert_eq!(metadata.last_resource_offset, None);
    assert_eq!(metadata.resource_count, 0);
    assert_eq!(metadata.area_watermark, 0);
    assert_eq!(metadata.realm_watermark, 0);
}

pub(crate) async fn should_allow_append_given_empty_body<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/payloads/empty-body";
    let begin_response = client
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin empty-body stream session");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client
        .send_and_receive(&build_stream_append(session_id, 0, b""), 2000)
        .await
        .expect("append empty-body stream event");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Expected success for stream append");

    let commit_response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("commit empty-body stream session");
    let (_msg_type, status, _data) = parse_stream_response(&commit_response);
    assert_eq!(status, 0, "Expected success for stream commit");

    // Act
    let response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read empty-body stream record");
    let read = parse_stream_read_response(&response);

    // Assert
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].body, Vec::<u8>::new());
}

pub(crate) async fn should_allow_append_given_metadata_only<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/payloads/metadata-only";
    let begin_response = client
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin metadata-only stream session");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client
        .send_and_receive(
            &build_stream_append_with_metadata(session_id, 0, b"", Some(b"meta")),
            2000,
        )
        .await
        .expect("append metadata-only stream event");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Expected success for stream append");

    let commit_response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("commit metadata-only stream session");
    let (_msg_type, status, _data) = parse_stream_response(&commit_response);
    assert_eq!(status, 0, "Expected success for stream commit");

    // Act
    let response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read metadata-only stream record");
    let read = parse_stream_read_response(&response);

    // Assert
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].body, Vec::<u8>::new());
    assert_eq!(read.records[0].metadata, Some(b"meta".to_vec()));
}

// Generic test helper for multiple sequential read operations
pub(crate) async fn should_handle_sequential_read_operations<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/sequential/main";

    commit_stream_record_with_offset(&mut client, route, 0, b"event-1").await;
    commit_stream_record_with_offset(&mut client, route, 1, b"event-2").await;
    commit_stream_record_with_offset(&mut client, route, 2, b"event-3").await;

    // Act
    let read1_frame = build_stream_read_with_options(route, 0, 1, None);
    let response1 = client
        .send_and_receive(&read1_frame, 2000)
        .await
        .expect("read 1");

    let read2_frame = build_stream_read_with_options(route, 1, 1, None);
    let response2 = client
        .send_and_receive(&read2_frame, 2000)
        .await
        .expect("read 2");

    let read3_frame = build_stream_read_with_options(route, 2, 1, None);
    let response3 = client
        .send_and_receive(&read3_frame, 2000)
        .await
        .expect("read 3");

    // Assert
    let read1 = parse_stream_read_response(&response1);
    let read2 = parse_stream_read_response(&response2);
    let read3 = parse_stream_read_response(&response3);
    assert_eq!(read1.records.len(), 1);
    assert_eq!(read1.records[0].body, b"event-1".to_vec());
    assert_eq!(read1.cursor.last_resource_offset, 0);
    assert!(read1.cursor.has_more);
    assert_eq!(read2.records.len(), 1);
    assert_eq!(read2.records[0].body, b"event-2".to_vec());
    assert_eq!(read2.cursor.last_resource_offset, 1);
    assert!(read2.cursor.has_more);
    assert_eq!(read3.records.len(), 1);
    assert_eq!(read3.records[0].body, b"event-3".to_vec());
    assert_eq!(read3.cursor.last_resource_offset, 2);
    assert!(!read3.cursor.has_more);
}

// Generic test helper for stream isolation
pub(crate) async fn should_isolate_streams_by_route<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route1 = "stream://test/app/stream1";
    let route2 = "stream://test/app/stream2";

    // Act - Append to stream 1
    commit_stream_record(&mut client, route1, b"data-1").await;

    // Act - Append to stream 2
    commit_stream_record(&mut client, route2, b"data-2").await;

    // Act - Read from stream 1
    let read_frame = build_stream_read(route1, 0);
    let response = client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("read");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Should isolate streams by route");
}
