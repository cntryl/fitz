use super::common::*;

pub(crate) async fn exercise_request_timeout_error_after_accept_tcp(server: &TestServer) {
    let worker_route = "rpc://test/services/timeout";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestClient::new(server.tcp_addr)
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestClient::new(server.tcp_addr)
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    let timeout_error = caller.recv_frame(2000).await.expect("timeout error");
    assert_rpc_timeout_error_frame(&timeout_error, delivered_request.correlation_id);
}

pub(crate) async fn exercise_request_timeout_error_after_accept_ws(server: &TestServer) {
    let worker_route = "rpc://test/services/timeout";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    let timeout_error = caller.recv_frame(2000).await.expect("timeout error");
    assert_rpc_timeout_error_frame(&timeout_error, delivered_request.correlation_id);

    worker.close().await.expect("close worker");
    caller.close().await.expect("close caller");
}

pub(crate) async fn exercise_wrong_correlation_error_after_accept_tcp(server: &TestServer) {
    let worker_route = "rpc://test/services/correlation";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestClient::new(server.tcp_addr)
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestClient::new(server.tcp_addr)
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");
    let wrong_correlation_id = uuid::Uuid::new_v4();
    assert_ne!(wrong_correlation_id, delivered_request.correlation_id);
    let wrong_response_frame = build_rpc_response_delivery(wrong_correlation_id, 0, true, b"wrong");

    worker
        .send_frame(&wrong_response_frame)
        .await
        .expect("send wrong correlation response");

    let correlation_error = worker.recv_frame(2000).await.expect("correlation error");
    assert_rpc_correlation_not_found_error_frame(&correlation_error, wrong_correlation_id);

    let timeout_error = caller.recv_frame(2000).await.expect("timeout error");
    assert_rpc_timeout_error_frame(&timeout_error, delivered_request.correlation_id);
}

pub(crate) async fn exercise_wrong_correlation_error_after_accept_ws(server: &TestServer) {
    let worker_route = "rpc://test/services/correlation";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");
    let wrong_correlation_id = uuid::Uuid::new_v4();
    assert_ne!(wrong_correlation_id, delivered_request.correlation_id);
    let wrong_response_frame = build_rpc_response_delivery(wrong_correlation_id, 0, true, b"wrong");

    worker
        .send_frame(&wrong_response_frame)
        .await
        .expect("send wrong correlation response");

    let correlation_error = worker.recv_frame(2000).await.expect("correlation error");
    assert_rpc_correlation_not_found_error_frame(&correlation_error, wrong_correlation_id);

    let timeout_error = caller.recv_frame(2000).await.expect("timeout error");
    assert_rpc_timeout_error_frame(&timeout_error, delivered_request.correlation_id);

    worker.close().await.expect("close worker");
    caller.close().await.expect("close caller");
}

pub(crate) async fn exercise_invalid_sequence_error_after_accept_tcp(server: &TestServer) {
    let worker_route = "rpc://test/services/invalid-sequence";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestClient::new(server.tcp_addr)
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestClient::new(server.tcp_addr)
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    let invalid_response_frame =
        build_rpc_response_delivery(delivered_request.correlation_id, 1, false, b"gap");
    worker
        .send_frame(&invalid_response_frame)
        .await
        .expect("send invalid sequence response");

    let caller_error = caller
        .recv_frame(2000)
        .await
        .expect("caller invalid sequence error");
    assert_rpc_invalid_sequence_error_frame(&caller_error, delivered_request.correlation_id);

    let worker_error = worker
        .recv_frame(2000)
        .await
        .expect("worker invalid sequence error");
    assert_rpc_invalid_sequence_error_frame(&worker_error, delivered_request.correlation_id);

    let late_response_frame =
        build_rpc_response_delivery(delivered_request.correlation_id, 0, true, b"late");
    worker
        .send_frame(&late_response_frame)
        .await
        .expect("send late response after invalid sequence");

    let orphan_error = worker
        .recv_frame(2000)
        .await
        .expect("correlation error after cleanup");
    assert_rpc_correlation_not_found_error_frame(&orphan_error, delivered_request.correlation_id);
}

pub(crate) async fn exercise_invalid_sequence_error_after_accept_ws(server: &TestServer) {
    let worker_route = "rpc://test/services/invalid-sequence";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    let invalid_response_frame =
        build_rpc_response_delivery(delivered_request.correlation_id, 1, false, b"gap");
    worker
        .send_frame(&invalid_response_frame)
        .await
        .expect("send invalid sequence response");

    let caller_error = caller
        .recv_frame(2000)
        .await
        .expect("caller invalid sequence error");
    assert_rpc_invalid_sequence_error_frame(&caller_error, delivered_request.correlation_id);

    let worker_error = worker
        .recv_frame(2000)
        .await
        .expect("worker invalid sequence error");
    assert_rpc_invalid_sequence_error_frame(&worker_error, delivered_request.correlation_id);

    let late_response_frame =
        build_rpc_response_delivery(delivered_request.correlation_id, 0, true, b"late");
    worker
        .send_frame(&late_response_frame)
        .await
        .expect("send late response after invalid sequence");

    let orphan_error = worker
        .recv_frame(2000)
        .await
        .expect("correlation error after cleanup");
    assert_rpc_correlation_not_found_error_frame(&orphan_error, delivered_request.correlation_id);

    worker.close().await.expect("close worker");
    caller.close().await.expect("close caller");
}

pub(crate) async fn exercise_worker_disconnect_error_after_accept_tcp(server: &TestServer) {
    let worker_route = "rpc://test/services/disconnect";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestClient::new(server.tcp_addr)
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestClient::new(server.tcp_addr)
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    drop(worker);
    server
        .wait_for_session_count(1)
        .await
        .expect("wait for worker disconnect");

    let disconnect_error = caller.recv_frame(2000).await.expect("disconnect error");
    assert_worker_disconnect_error_frame(&disconnect_error, delivered_request.correlation_id);
}

pub(crate) async fn exercise_worker_disconnect_error_after_accept_ws(server: &TestServer) {
    let worker_route = "rpc://test/services/disconnect";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    worker.close().await.expect("close worker");
    server
        .wait_for_session_count(1)
        .await
        .expect("wait for worker disconnect");

    let disconnect_error = caller.recv_frame(2000).await.expect("disconnect error");
    assert_worker_disconnect_error_frame(&disconnect_error, delivered_request.correlation_id);

    caller.close().await.expect("close caller");
}

pub(crate) async fn exercise_worker_unregister_error_after_accept_tcp(server: &TestServer) {
    let worker_route = "rpc://test/services/unregister";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let unsubscribe_frame = build_rpc_unsubscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestClient::new(server.tcp_addr)
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestClient::new(server.tcp_addr)
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    worker
        .send_frame(&unsubscribe_frame)
        .await
        .expect("unsubscribe worker");
    let unsubscribe_response = worker.recv_frame(2000).await.expect("unsubscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&unsubscribe_response);
    assert_eq!(status, 0);
    server
        .wait_for_session_count(2)
        .await
        .expect("worker should remain connected after unsubscribe");

    let unregister_error = caller.recv_frame(2000).await.expect("unregister error");
    assert_worker_disconnect_error_frame(&unregister_error, delivered_request.correlation_id);
}

pub(crate) async fn exercise_worker_unregister_error_after_accept_ws(server: &TestServer) {
    let worker_route = "rpc://test/services/unregister";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let unsubscribe_frame = build_rpc_unsubscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    worker
        .send_frame(&unsubscribe_frame)
        .await
        .expect("unsubscribe worker");
    let unsubscribe_response = worker.recv_frame(2000).await.expect("unsubscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&unsubscribe_response);
    assert_eq!(status, 0);
    server
        .wait_for_session_count(2)
        .await
        .expect("worker should remain connected after unsubscribe");

    let unregister_error = caller.recv_frame(2000).await.expect("unregister error");
    assert_worker_disconnect_error_frame(&unregister_error, delivered_request.correlation_id);

    worker.close().await.expect("close worker");
    caller.close().await.expect("close caller");
}

pub(crate) async fn exercise_retained_worker_route_after_unsubscribe_tcp(server: &TestServer) {
    let removed_route = "rpc://test/services/unregister/removed";
    let retained_route = "rpc://test/services/unregister/retained";
    let subscribe_removed_frame = build_rpc_subscribe(removed_route);
    let subscribe_retained_frame = build_rpc_subscribe(retained_route);
    let unsubscribe_removed_frame = build_rpc_unsubscribe(removed_route);
    let request_frame = build_rpc_request(retained_route, "getUser", b"user-123");

    let mut worker = TestClient::new(server.tcp_addr)
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_removed_frame)
        .await
        .expect("subscribe removed route");
    let subscribe_removed_response = worker
        .recv_frame(2000)
        .await
        .expect("subscribe removed ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_removed_response);
    assert_eq!(status, 0);

    worker
        .send_frame(&subscribe_retained_frame)
        .await
        .expect("subscribe retained route");
    let subscribe_retained_response = worker
        .recv_frame(2000)
        .await
        .expect("subscribe retained ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_retained_response);
    assert_eq!(status, 0);

    let mut caller = TestClient::new(server.tcp_addr)
        .await
        .expect("connect caller");

    worker
        .send_frame(&unsubscribe_removed_frame)
        .await
        .expect("unsubscribe removed route");
    let unsubscribe_response = worker.recv_frame(2000).await.expect("unsubscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&unsubscribe_response);
    assert_eq!(status, 0);
    server
        .wait_for_session_count(2)
        .await
        .expect("worker should remain connected after one route unsubscribe");

    caller
        .send_frame(&request_frame)
        .await
        .expect("send retained route request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");
    assert_eq!(delivered_request.route, retained_route);
    assert_eq!(delivered_request.body.as_slice(), b"user-123");

    let response_frame =
        build_rpc_response_delivery(delivered_request.correlation_id, 0, true, b"ok");
    worker
        .send_frame(&response_frame)
        .await
        .expect("send retained route response");

    let forwarded_response = caller.recv_frame(2000).await.expect("forwarded response");
    let forwarded_response =
        parse_rpc_response_delivery(&forwarded_response).expect("parse forwarded response");
    assert_eq!(
        forwarded_response.correlation_id,
        delivered_request.correlation_id
    );
    assert_eq!(forwarded_response.seq, 0);
    assert!(forwarded_response.stream_end);
    assert_eq!(forwarded_response.body.as_slice(), b"ok");
}

pub(crate) async fn exercise_retained_worker_route_after_unsubscribe_ws(server: &TestServer) {
    let removed_route = "rpc://test/services/unregister/removed";
    let retained_route = "rpc://test/services/unregister/retained";
    let subscribe_removed_frame = build_rpc_subscribe(removed_route);
    let subscribe_retained_frame = build_rpc_subscribe(retained_route);
    let unsubscribe_removed_frame = build_rpc_unsubscribe(removed_route);
    let request_frame = build_rpc_request(retained_route, "getUser", b"user-123");

    let mut worker = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_removed_frame)
        .await
        .expect("subscribe removed route");
    let subscribe_removed_response = worker
        .recv_frame(2000)
        .await
        .expect("subscribe removed ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_removed_response);
    assert_eq!(status, 0);

    worker
        .send_frame(&subscribe_retained_frame)
        .await
        .expect("subscribe retained route");
    let subscribe_retained_response = worker
        .recv_frame(2000)
        .await
        .expect("subscribe retained ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_retained_response);
    assert_eq!(status, 0);

    let mut caller = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect caller");

    worker
        .send_frame(&unsubscribe_removed_frame)
        .await
        .expect("unsubscribe removed route");
    let unsubscribe_response = worker.recv_frame(2000).await.expect("unsubscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&unsubscribe_response);
    assert_eq!(status, 0);
    server
        .wait_for_session_count(2)
        .await
        .expect("worker should remain connected after one route unsubscribe");

    caller
        .send_frame(&request_frame)
        .await
        .expect("send retained route request");

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");
    assert_eq!(delivered_request.route, retained_route);
    assert_eq!(delivered_request.body.as_slice(), b"user-123");

    let response_frame =
        build_rpc_response_delivery(delivered_request.correlation_id, 0, true, b"ok");
    worker
        .send_frame(&response_frame)
        .await
        .expect("send retained route response");

    let forwarded_response = caller.recv_frame(2000).await.expect("forwarded response");
    let forwarded_response =
        parse_rpc_response_delivery(&forwarded_response).expect("parse forwarded response");
    assert_eq!(
        forwarded_response.correlation_id,
        delivered_request.correlation_id
    );
    assert_eq!(forwarded_response.seq, 0);
    assert!(forwarded_response.stream_end);
    assert_eq!(forwarded_response.body.as_slice(), b"ok");

    worker.close().await.expect("close worker");
    caller.close().await.expect("close caller");
}

#[tokio::test]
#[serial]
pub(crate) async fn should_return_worker_disconnect_error_after_accept_tcp() {
    let server = TestServer::start().await.expect("start");
    exercise_worker_disconnect_error_after_accept_tcp(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_return_worker_disconnect_error_after_accept_ws() {
    let server = TestServer::start().await.expect("start");
    exercise_worker_disconnect_error_after_accept_ws(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_return_worker_disconnect_error_after_unsubscribe_tcp() {
    let server = TestServer::start().await.expect("start");
    exercise_worker_unregister_error_after_accept_tcp(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_return_worker_disconnect_error_after_unsubscribe_ws() {
    let server = TestServer::start().await.expect("start");
    exercise_worker_unregister_error_after_accept_ws(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_retain_other_worker_route_after_unsubscribe_tcp() {
    let server = TestServer::start().await.expect("start");
    exercise_retained_worker_route_after_unsubscribe_tcp(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_retain_other_worker_route_after_unsubscribe_ws() {
    let server = TestServer::start().await.expect("start");
    exercise_retained_worker_route_after_unsubscribe_ws(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_return_rpc_timeout_error_after_accept_tcp() {
    let server = TestServer::start_with_rpc_timeout(Duration::from_millis(150))
        .await
        .expect("start");
    exercise_request_timeout_error_after_accept_tcp(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_return_rpc_timeout_error_after_accept_ws() {
    let server = TestServer::start_with_rpc_timeout(Duration::from_millis(150))
        .await
        .expect("start");
    exercise_request_timeout_error_after_accept_ws(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_wrong_correlation_response_after_accept_tcp() {
    let server = TestServer::start_with_rpc_timeout(Duration::from_millis(150))
        .await
        .expect("start");
    exercise_wrong_correlation_error_after_accept_tcp(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_wrong_correlation_response_after_accept_ws() {
    let server = TestServer::start_with_rpc_timeout(Duration::from_millis(150))
        .await
        .expect("start");
    exercise_wrong_correlation_error_after_accept_ws(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_invalid_sequence_response_after_accept_tcp() {
    let server = TestServer::start().await.expect("start");
    exercise_invalid_sequence_error_after_accept_tcp(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_invalid_sequence_response_after_accept_ws() {
    let server = TestServer::start().await.expect("start");
    exercise_invalid_sequence_error_after_accept_ws(&server).await;
}
