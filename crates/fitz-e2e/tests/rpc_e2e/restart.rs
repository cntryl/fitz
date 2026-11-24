use super::common::*;

pub(crate) async fn exercise_worker_reregistration_after_broker_restart_tcp() {
    let worker_route = "rpc://test/services/restart-reregister";

    {
        let first_server = TestServer::start().await.expect("start first server");
        let subscribe_frame = build_rpc_subscribe(worker_route);

        let mut worker = TestClient::new(first_server.tcp_addr)
            .await
            .expect("connect first worker");
        worker
            .send_frame(&subscribe_frame)
            .await
            .expect("subscribe first worker");
        let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
        let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
        assert_eq!(status, 0);
        assert_eq!(
            first_server.runtime.rpc_workers_registered(),
            1,
            "Expected one worker registration before restart"
        );
    }

    let restarted_server = TestServer::start().await.expect("start restarted server");
    assert_eq!(
        restarted_server.runtime.rpc_workers_registered(),
        0,
        "Worker registrations should be lost on broker restart"
    );
    assert_eq!(
        restarted_server.runtime.rpc_requests_pending(),
        0,
        "Pending RPC requests should start empty after restart"
    );

    let mut caller = TestClient::new(restarted_server.tcp_addr)
        .await
        .expect("connect caller after restart");
    let request_before_reregister = build_rpc_request(worker_route, "getUser", b"user-123");
    caller
        .send_frame(&request_before_reregister)
        .await
        .expect("send request before reregister");
    let no_worker_response = caller
        .recv_frame(2000)
        .await
        .expect("route-not-registered response");
    assert_rpc_route_not_registered_error_response(&no_worker_response);

    let subscribe_frame = build_rpc_subscribe(worker_route);
    let mut restarted_worker = TestClient::new(restarted_server.tcp_addr)
        .await
        .expect("connect restarted worker");
    restarted_worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe restarted worker");
    let subscribe_response = restarted_worker
        .recv_frame(2000)
        .await
        .expect("subscribe ack after restart");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);
    assert_eq!(
        restarted_server.runtime.rpc_workers_registered(),
        1,
        "Worker must re-register before it can receive new requests"
    );

    let request_after_reregister = build_rpc_request(worker_route, "getUser", b"user-456");
    caller
        .send_frame(&request_after_reregister)
        .await
        .expect("send request after reregister");

    let delivered_request = restarted_worker
        .recv_frame(2000)
        .await
        .expect("request delivery after reregister");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");
    assert_eq!(delivered_request.route, worker_route);
    assert_eq!(delivered_request.body.as_slice(), b"user-456");

    let response_frame =
        build_rpc_response_delivery(delivered_request.correlation_id, 0, true, b"ok");
    restarted_worker
        .send_frame(&response_frame)
        .await
        .expect("send worker response");

    let forwarded_response = caller.recv_frame(2000).await.expect("forwarded response");
    assert_forwarded_rpc_response_frame(
        &forwarded_response,
        delivered_request.correlation_id,
        b"ok",
    );
}

pub(crate) async fn exercise_worker_reregistration_after_broker_restart_ws() {
    let worker_route = "rpc://test/services/restart-reregister";

    {
        let first_server = TestServer::start().await.expect("start first server");
        let subscribe_frame = build_rpc_subscribe(worker_route);

        let mut worker = TestWebSocketClient::connect(&format!("ws://{}", first_server.ws_addr))
            .await
            .expect("connect first worker");
        worker
            .send_frame(&subscribe_frame)
            .await
            .expect("subscribe first worker");
        let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
        let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
        assert_eq!(status, 0);
        assert_eq!(
            first_server.runtime.rpc_workers_registered(),
            1,
            "Expected one worker registration before restart"
        );
    }

    let restarted_server = TestServer::start().await.expect("start restarted server");
    assert_eq!(
        restarted_server.runtime.rpc_workers_registered(),
        0,
        "Worker registrations should be lost on broker restart"
    );
    assert_eq!(
        restarted_server.runtime.rpc_requests_pending(),
        0,
        "Pending RPC requests should start empty after restart"
    );

    let mut caller = TestWebSocketClient::connect(&format!("ws://{}", restarted_server.ws_addr))
        .await
        .expect("connect caller after restart");
    let request_before_reregister = build_rpc_request(worker_route, "getUser", b"user-123");
    caller
        .send_frame(&request_before_reregister)
        .await
        .expect("send request before reregister");
    let no_worker_response = caller
        .recv_frame(2000)
        .await
        .expect("route-not-registered response");
    assert_rpc_route_not_registered_error_response(&no_worker_response);

    let subscribe_frame = build_rpc_subscribe(worker_route);
    let mut restarted_worker =
        TestWebSocketClient::connect(&format!("ws://{}", restarted_server.ws_addr))
            .await
            .expect("connect restarted worker");
    restarted_worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe restarted worker");
    let subscribe_response = restarted_worker
        .recv_frame(2000)
        .await
        .expect("subscribe ack after restart");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);
    assert_eq!(
        restarted_server.runtime.rpc_workers_registered(),
        1,
        "Worker must re-register before it can receive new requests"
    );

    let request_after_reregister = build_rpc_request(worker_route, "getUser", b"user-456");
    caller
        .send_frame(&request_after_reregister)
        .await
        .expect("send request after reregister");

    let delivered_request = restarted_worker
        .recv_frame(2000)
        .await
        .expect("request delivery after reregister");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");
    assert_eq!(delivered_request.route, worker_route);
    assert_eq!(delivered_request.body.as_slice(), b"user-456");

    let response_frame =
        build_rpc_response_delivery(delivered_request.correlation_id, 0, true, b"ok");
    restarted_worker
        .send_frame(&response_frame)
        .await
        .expect("send worker response");

    let forwarded_response = caller.recv_frame(2000).await.expect("forwarded response");
    assert_forwarded_rpc_response_frame(
        &forwarded_response,
        delivered_request.correlation_id,
        b"ok",
    );
}

pub(crate) async fn exercise_pending_request_loss_after_broker_restart_tcp() {
    let worker_route = "rpc://test/services/restart-pending";

    {
        let first_server = TestServer::start().await.expect("start first server");
        let subscribe_frame = build_rpc_subscribe(worker_route);
        let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

        let mut worker = TestClient::new(first_server.tcp_addr)
            .await
            .expect("connect first worker");
        worker
            .send_frame(&subscribe_frame)
            .await
            .expect("subscribe first worker");
        let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
        let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
        assert_eq!(status, 0);

        let mut caller = TestClient::new(first_server.tcp_addr)
            .await
            .expect("connect first caller");
        caller
            .send_frame(&request_frame)
            .await
            .expect("send in-flight request");

        let _delivered_request = worker.recv_frame(2000).await.expect("request delivery");
        assert_eq!(
            first_server.runtime.rpc_requests_pending(),
            1,
            "Expected one pending RPC request before restart"
        );
    }

    let restarted_server = TestServer::start().await.expect("start restarted server");
    assert_eq!(
        restarted_server.runtime.rpc_requests_pending(),
        0,
        "Pending RPC requests should be lost on broker restart"
    );
    assert_eq!(
        restarted_server.runtime.rpc_workers_registered(),
        0,
        "Worker registrations should not survive broker restart"
    );

    let subscribe_frame = build_rpc_subscribe(worker_route);
    let mut worker = TestClient::new(restarted_server.tcp_addr)
        .await
        .expect("connect restarted worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe restarted worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);
    assert!(
        worker.recv_frame(200).await.is_err(),
        "Pending request should not be replayed after broker restart"
    );

    let request_frame = build_rpc_request(worker_route, "getUser", b"user-456");
    let mut caller = TestClient::new(restarted_server.tcp_addr)
        .await
        .expect("connect restarted caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send fresh request");

    let delivered_request = worker
        .recv_frame(2000)
        .await
        .expect("fresh request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");
    assert_eq!(delivered_request.body.as_slice(), b"user-456");
}

pub(crate) async fn exercise_pending_request_loss_after_broker_restart_ws() {
    let worker_route = "rpc://test/services/restart-pending";

    {
        let first_server = TestServer::start().await.expect("start first server");
        let subscribe_frame = build_rpc_subscribe(worker_route);
        let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

        let mut worker = TestWebSocketClient::connect(&format!("ws://{}", first_server.ws_addr))
            .await
            .expect("connect first worker");
        worker
            .send_frame(&subscribe_frame)
            .await
            .expect("subscribe first worker");
        let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
        let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
        assert_eq!(status, 0);

        let mut caller = TestWebSocketClient::connect(&format!("ws://{}", first_server.ws_addr))
            .await
            .expect("connect first caller");
        caller
            .send_frame(&request_frame)
            .await
            .expect("send in-flight request");

        let _delivered_request = worker.recv_frame(2000).await.expect("request delivery");
        assert_eq!(
            first_server.runtime.rpc_requests_pending(),
            1,
            "Expected one pending RPC request before restart"
        );
    }

    let restarted_server = TestServer::start().await.expect("start restarted server");
    assert_eq!(
        restarted_server.runtime.rpc_requests_pending(),
        0,
        "Pending RPC requests should be lost on broker restart"
    );
    assert_eq!(
        restarted_server.runtime.rpc_workers_registered(),
        0,
        "Worker registrations should not survive broker restart"
    );

    let subscribe_frame = build_rpc_subscribe(worker_route);
    let mut worker = TestWebSocketClient::connect(&format!("ws://{}", restarted_server.ws_addr))
        .await
        .expect("connect restarted worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe restarted worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);
    assert!(
        worker.recv_frame(200).await.is_err(),
        "Pending request should not be replayed after broker restart"
    );

    let request_frame = build_rpc_request(worker_route, "getUser", b"user-456");
    let mut caller = TestWebSocketClient::connect(&format!("ws://{}", restarted_server.ws_addr))
        .await
        .expect("connect restarted caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send fresh request");

    let delivered_request = worker
        .recv_frame(2000)
        .await
        .expect("fresh request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");
    assert_eq!(delivered_request.body.as_slice(), b"user-456");
}

#[tokio::test]
#[serial]
pub(crate) async fn should_require_worker_reregistration_after_broker_restart_tcp() {
    exercise_worker_reregistration_after_broker_restart_tcp().await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_require_worker_reregistration_after_broker_restart_ws() {
    exercise_worker_reregistration_after_broker_restart_ws().await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_drop_pending_requests_on_broker_restart_tcp() {
    exercise_pending_request_loss_after_broker_restart_tcp().await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_drop_pending_requests_on_broker_restart_ws() {
    exercise_pending_request_loss_after_broker_restart_ws().await;
}
