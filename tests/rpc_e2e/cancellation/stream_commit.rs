//! Cancellation ACKs queued behind real Stream COMMIT frames on shared connections.

use super::*;
use fitz::protocol::payload_codec::PayloadDecoder;

const RPC_ROUTE: &str = "rpc://cancel/service/stream-commit";
const STREAM_ROUTE: &str = "stream://cancel/events/shared-worker";
const STREAM_BODY: &[u8] = b"committed while RPC cancellation waits";
const SURVIVING_RESPONSE: &[u8] = b"second call survived the held COMMIT";
const HOLD_PAST_GRACE: Duration = Duration::from_secs(6);

fn successful_stream_payload(frame: &[u8], expected_type: u16) -> Vec<u8> {
    let mut parser = TlvFrameParser::new(frame);
    let (message_type, payload) = parser.next_field().expect("Stream response");
    assert_eq!(message_type, expected_type);
    assert!(parser.next_field().is_none());
    assert_eq!(payload.first(), Some(&0), "Stream operation must succeed");
    payload
}

async fn stage_stream_record<C>(worker: &mut C) -> u64
where
    C: RpcConnector,
{
    let begin = worker
        .request(&build_stream_begin(STREAM_ROUTE), 2000)
        .await
        .expect("begin Stream on RPC worker connection");
    let session_id = parse_stream_session_id(&successful_stream_payload(&begin, 600))
        .expect("real Stream append session");
    let append = worker
        .request(&build_stream_append(session_id, 0, STREAM_BODY), 2000)
        .await
        .expect("append Stream on RPC worker connection");
    let _ = successful_stream_payload(&append, 601);
    session_id
}

async fn dispatch_call<C>(caller: &mut C, worker: &mut C, caller_id: Uuid) -> Uuid
where
    C: RpcConnector + FrameReceivingConnector,
{
    caller
        .send_frame(&rpc_request(caller_id, RPC_ROUTE, 30_000))
        .await
        .expect("send RPC request");
    let delivery = worker.recv_frame(2000).await.expect("dispatch RPC call");
    let request = parse_rpc_request_delivery(&delivery).expect("RPC worker request");
    assert_eq!(request.route, RPC_ROUTE);
    assert_ne!(request.correlation_id, caller_id);
    request.correlation_id
}

async fn cancel_and_queue_cleanup_ack<C>(
    caller: &mut C,
    worker: &mut C,
    caller_id: Uuid,
    worker_id: Uuid,
) where
    C: RpcConnector + FrameReceivingConnector,
{
    caller
        .send_frame(&caller_cancel(caller_id))
        .await
        .expect("cancel first in-flight call");
    let cancellation = worker
        .recv_frame(2000)
        .await
        .expect("receive worker cancellation");
    assert_lifecycle(&cancellation, 2, worker_id, 1);
    worker
        .send_frame(&worker_cleanup_ack(worker_id))
        .await
        .expect("queue cleanup ACK behind COMMIT");
    let cancelled = caller
        .recv_frame(2000)
        .await
        .expect("caller cancellation result");
    assert_lifecycle(&cancelled, 4, caller_id, 2);
}

async fn force_due_rpc_sweep<C>(caller: &mut C)
where
    C: RpcConnector + FrameReceivingConnector,
{
    let probe_id = Uuid::new_v4();
    caller
        .send_frame(&rpc_request(
            probe_id,
            "rpc://cancel/service/missing",
            30_000,
        ))
        .await
        .expect("force a due RPC timeout sweep while COMMIT remains blocked");
    let probe = caller
        .recv_frame(2000)
        .await
        .expect("missing-route probe response");
    assert_rpc_route_not_registered_error_response(&probe);
    assert_eq!(
        parse_rpc_response_delivery(&probe)
            .expect("parse probe response")
            .correlation_id,
        probe_id
    );
}

fn assert_committed_stream_record(frame: &[u8]) {
    let payload = successful_stream_payload(frame, 604);
    let mut response = PayloadDecoder::new(&payload);
    assert_eq!(response.get_u8().expect("Stream status"), 0);
    assert_eq!(response.get_optional_u64().expect("read session id"), None);
    let data = response.get_bytes().expect("Stream READ data");
    assert!(response.is_complete());

    let mut record = PayloadDecoder::new(&data);
    assert_eq!(record.get_u32().expect("record count"), 1);
    assert_eq!(record.get_string().expect("record route"), STREAM_ROUTE);
    assert_eq!(record.get_u8().expect("record kind"), 0);
    assert_eq!(record.get_u64().expect("resource offset"), 0);
    let _area_offset = record.get_optional_u64().expect("area offset");
    let _realm_offset = record.get_optional_u64().expect("realm offset");
    assert_eq!(
        record.get_bytes().expect("committed body").as_ref(),
        STREAM_BODY
    );
    assert_eq!(record.get_optional_bytes().expect("record metadata"), None);
    record.get_u64().expect("record creation time");
    assert_eq!(record.get_u64().expect("cursor resource offset"), 0);
    let _cursor_area_offset = record.get_optional_u64().expect("cursor area offset");
    let _cursor_realm_offset = record.get_optional_u64().expect("cursor realm offset");
    assert_eq!(record.get_u8().expect("cursor has more"), 0);
    assert!(record.is_complete());
}

async fn should_keep_worker_during_stream_commit_past_cancellation_grace<C>(server: &TestServer)
where
    C: RpcConnector + FrameReceivingConnector,
{
    // Arrange
    let mut worker = C::connect(server).await.expect("connect shared worker");
    let registration = worker
        .request(&subscribe_cancellation_worker(RPC_ROUTE, 2), 2000)
        .await
        .expect("register cancellation-aware worker with two credits");
    let (_, status, _) = parse_rpc_response(&registration);
    assert_eq!(status, 0);
    let mut caller = C::connect(server).await.expect("connect caller");
    let cancelled_id = Uuid::new_v4();
    let surviving_id = Uuid::new_v4();
    let cancelled_worker_id = dispatch_call(&mut caller, &mut worker, cancelled_id).await;
    let surviving_worker_id = dispatch_call(&mut caller, &mut worker, surviving_id).await;
    assert_ne!(cancelled_worker_id, surviving_worker_id);
    let stream_session = stage_stream_record(&mut worker).await;
    let mut commit = server
        .block_next_stream_commit()
        .expect("install COMMIT gate");

    // Act
    worker
        .send_frame(&build_stream_commit(stream_session))
        .await
        .expect("send COMMIT on shared worker connection");
    commit
        .wait_until_blocked(fitz::testkit::scaled_test_timeout(Duration::from_secs(2)))
        .await
        .expect("worker frame loop waits for real COMMIT dispatch");
    cancel_and_queue_cleanup_ack(&mut caller, &mut worker, cancelled_id, cancelled_worker_id).await;

    // The five-second grace and two 250 ms sweeps pass with ACK still unread.
    tokio::time::sleep(HOLD_PAST_GRACE).await;
    force_due_rpc_sweep(&mut caller).await;
    commit.release();

    let committed = worker
        .recv_frame(2000)
        .await
        .expect("Stream COMMIT response");
    let _ = successful_stream_payload(&committed, 602);
    worker
        .send_frame(&build_rpc_response_delivery(
            surviving_worker_id,
            0,
            true,
            SURVIVING_RESPONSE,
        ))
        .await
        .expect("finish second in-flight call on original worker connection");
    let survived = caller.recv_frame(2000).await.expect("second call response");
    let readback = worker
        .request(&build_stream_read(STREAM_ROUTE, 0), 2000)
        .await
        .expect("read committed Stream through original worker connection");

    // Assert
    assert_forwarded_rpc_response_frame(&survived, surviving_id, SURVIVING_RESPONSE);
    assert_committed_stream_record(&readback);
    assert!(server.runtime.rpc_list_pending(None).is_empty());
    assert_eq!(server.runtime.rpc_list_workers(None).len(), 1);
}

#[tokio::test]
#[serial]
async fn should_keep_rpc_worker_during_stream_commit_past_cancellation_grace_over_tcp() {
    // Arrange
    let server = TestServer::start().await.expect("start");

    // Act
    let completed = tokio::time::timeout(
        fitz::testkit::scaled_test_timeout(Duration::from_secs(30)),
        should_keep_worker_during_stream_commit_past_cancellation_grace::<TcpRpcConnector>(&server),
    )
    .await;

    // Assert
    completed.expect("bounded TCP Stream COMMIT cancellation scenario");
}

#[tokio::test]
#[serial]
async fn should_keep_rpc_worker_during_stream_commit_past_cancellation_grace_over_websocket() {
    // Arrange
    let server = TestServer::start().await.expect("start");

    // Act
    let completed = tokio::time::timeout(
        fitz::testkit::scaled_test_timeout(Duration::from_secs(30)),
        should_keep_worker_during_stream_commit_past_cancellation_grace::<WsRpcConnector>(&server),
    )
    .await;

    // Assert
    completed.expect("bounded WebSocket Stream COMMIT cancellation scenario");
}
