use bytes::Bytes;
use fitz::benchkit::{build_rpc_request, build_rpc_response_frame_bytes, shared_bench_runtime};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{TestClient, TestWebSocketClient};
use std::collections::HashMap;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;
use uuid::Uuid;

pub(crate) struct NetworkRequestFrame {
    pub(crate) frame: Bytes,
    pub(crate) correlation_id: Uuid,
    pub(crate) body: Bytes,
}

struct RpcRequestParts {
    correlation_id: Uuid,
    body: Bytes,
}

struct RpcResponseParts {
    correlation_id: Uuid,
    seq: u64,
    body: Bytes,
    stream_end: bool,
}

pub(crate) struct RpcRequesterDriver {
    pub(crate) command_tx: tokio::sync::mpsc::UnboundedSender<usize>,
    pub(crate) handle: tokio::task::JoinHandle<()>,
}

fn read_u32(input: &[u8], offset: &mut usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let bytes = input.get(*offset..end)?;
    *offset = end;
    Some(u32::from_be_bytes(bytes.try_into().ok()?))
}

fn read_u64(input: &[u8], offset: &mut usize) -> Option<u64> {
    let end = offset.checked_add(8)?;
    let bytes = input.get(*offset..end)?;
    *offset = end;
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

fn read_len_prefixed_range(input: &[u8], offset: &mut usize) -> Option<(usize, usize)> {
    let len = usize::try_from(read_u32(input, offset)?).ok()?;
    let start = *offset;
    let end = start.checked_add(len)?;
    input.get(start..end)?;
    *offset = end;
    Some((start, end))
}

fn read_uuid_field(input: &[u8], offset: &mut usize) -> Option<Uuid> {
    let end = offset.checked_add(16)?;
    let uuid_bytes = input.get(*offset..end)?;
    *offset = end;
    let uuid_array: [u8; 16] = uuid_bytes.try_into().ok()?;
    Some(Uuid::from_bytes(uuid_array))
}

fn single_tlv_payload_range(frame: &[u8]) -> Option<(u16, usize, usize)> {
    const ESCAPE_MARKER: u8 = 0xFF;

    let mut offset = 0usize;
    let msg_type = if *frame.get(offset)? == ESCAPE_MARKER {
        let end = offset.checked_add(3)?;
        let bytes = frame.get(offset + 1..end)?;
        offset = end;
        u16::from_be_bytes(bytes.try_into().ok()?)
    } else {
        let value = u16::from(*frame.get(offset)?);
        offset += 1;
        value
    };

    let len_end = offset.checked_add(2)?;
    let len_bytes = frame.get(offset..len_end)?;
    offset = len_end;
    let len = usize::from(u16::from_be_bytes(len_bytes.try_into().ok()?));
    let end = offset.checked_add(len)?;
    frame.get(offset..end)?;
    Some((msg_type, offset, end))
}

pub(crate) fn try_parse_rpc_request_payload_parts(payload: &Bytes) -> Option<(Uuid, Bytes)> {
    let mut offset = 0usize;
    let correlation_id = read_uuid_field(payload.as_ref(), &mut offset)?;
    read_len_prefixed_range(payload.as_ref(), &mut offset)?;
    let (body_start, body_end) = read_len_prefixed_range(payload.as_ref(), &mut offset)?;
    if offset != payload.len() {
        return None;
    }

    Some((correlation_id, payload.slice(body_start..body_end)))
}

fn try_parse_rpc_request_frame_parts(frame: &Bytes) -> Option<RpcRequestParts> {
    let (msg_type, payload_start, payload_end) = single_tlv_payload_range(frame.as_ref())?;
    if msg_type != 302 {
        return None;
    }

    let payload = frame.slice(payload_start..payload_end);
    let (correlation_id, body) = try_parse_rpc_request_payload_parts(&payload)?;
    Some(RpcRequestParts {
        correlation_id,
        body,
    })
}

fn try_parse_rpc_response_payload_parts(payload: &Bytes) -> Option<RpcResponseParts> {
    let mut offset = 0usize;
    let correlation_id = read_uuid_field(payload.as_ref(), &mut offset)?;
    let seq = read_u64(payload.as_ref(), &mut offset)?;
    let flags = *payload.get(offset)?;
    offset += 1;
    let (body_start, body_end) = read_len_prefixed_range(payload.as_ref(), &mut offset)?;
    if offset != payload.len() {
        return None;
    }

    Some(RpcResponseParts {
        correlation_id,
        seq,
        body: payload.slice(body_start..body_end),
        stream_end: flags & 0x01 != 0,
    })
}

fn try_parse_rpc_response_frame_parts(frame: &Bytes) -> Option<RpcResponseParts> {
    let (msg_type, payload_start, payload_end) = single_tlv_payload_range(frame.as_ref())?;
    if msg_type != 303 {
        return None;
    }

    let payload = frame.slice(payload_start..payload_end);
    try_parse_rpc_response_payload_parts(&payload)
}

pub(crate) fn build_network_request_frame(
    route: &str,
    payload: &[u8],
    _family: RouteFamily,
) -> NetworkRequestFrame {
    let frame = Bytes::from(build_rpc_request(route, payload));
    let request = try_parse_rpc_request_frame_parts(&frame).expect("rpc request frame");

    NetworkRequestFrame {
        frame,
        correlation_id: request.correlation_id,
        body: request.body,
    }
}

pub(crate) fn build_network_request_frame_ring(
    route: &str,
    payload: &[u8],
    family: RouteFamily,
    count: usize,
) -> Vec<NetworkRequestFrame> {
    (0..count)
        .map(|_| build_network_request_frame(route, payload, family))
        .collect()
}

fn validate_rpc_worker_response(
    response: &RpcResponseParts,
    expected_correlation_id: Uuid,
    expected_body: &[u8],
) -> Result<(), String> {
    if response.correlation_id != expected_correlation_id {
        return Err(format!(
            "unexpected rpc correlation id: expected {expected_correlation_id}, got {}",
            response.correlation_id
        ));
    }
    if response.seq != 0 {
        return Err(format!(
            "single-response bench should emit seq 0, got {}",
            response.seq
        ));
    }
    if response.body.as_ref() != expected_body {
        return Err(format!(
            "unexpected rpc response body: expected {expected_body:?}, got {:?}",
            response.body.as_ref()
        ));
    }
    if !response.stream_end {
        return Err("single-response bench should end the stream".to_string());
    }

    Ok(())
}

pub(crate) fn assert_requester_inbox_contains_worker_response(
    frames: Vec<fitz::protocol::frame_context::FrameContext>,
    expected_correlation_id: Uuid,
    expected_body: &[u8],
) {
    for frame in frames {
        if frame.msg_type.as_u16() != 303 {
            continue;
        }

        if let Some(response) = try_parse_rpc_response_payload_parts(&frame.payload) {
            validate_rpc_worker_response(&response, expected_correlation_id, expected_body)
                .expect("valid rpc worker response");
            return;
        }

        panic!("failed to parse rpc response frame");
    }

    panic!("expected worker rpc response in requester inbox");
}

async fn try_request_until_worker_response_tcp(
    client: &mut TestClient,
    request_frame: &NetworkRequestFrame,
) -> Result<(), String> {
    client
        .send_frame_bytes(request_frame.frame.clone())
        .await
        .map_err(|error| format!("send rpc request: {error}"))?;

    for _ in 0..4 {
        let frame = client
            .recv_frame_bytes_without_timeout()
            .await
            .map_err(|error| format!("receive rpc response: {error}"))?;
        if let Some(response) = try_parse_rpc_response_frame_parts(&frame) {
            validate_rpc_worker_response(
                &response,
                request_frame.correlation_id,
                request_frame.body.as_ref(),
            )?;
            return Ok(());
        }
    }

    Err("expected worker rpc response frame over tcp".to_string())
}

pub(crate) async fn request_until_worker_response_tcp(
    client: &mut TestClient,
    request_frame: &NetworkRequestFrame,
) {
    try_request_until_worker_response_tcp(client, request_frame)
        .await
        .expect("rpc tcp worker response");
}

async fn try_request_until_worker_response_ws(
    client: &mut TestWebSocketClient,
    request_frame: &NetworkRequestFrame,
) -> Result<(), String> {
    client
        .send_frame_bytes(request_frame.frame.clone())
        .await
        .map_err(|error| format!("send rpc request: {error}"))?;

    for _ in 0..4 {
        let frame = client
            .recv_frame_bytes_without_timeout()
            .await
            .map_err(|error| format!("receive rpc response: {error}"))?;
        if let Some(response) = try_parse_rpc_response_frame_parts(&frame) {
            validate_rpc_worker_response(
                &response,
                request_frame.correlation_id,
                request_frame.body.as_ref(),
            )?;
            return Ok(());
        }
    }

    Err("expected worker rpc response frame over websocket".to_string())
}

pub(crate) async fn request_until_worker_response_ws(
    client: &mut TestWebSocketClient,
    request_frame: &NetworkRequestFrame,
) {
    try_request_until_worker_response_ws(client, request_frame)
        .await
        .expect("rpc websocket worker response");
}

pub(crate) fn spawn_rpc_tcp_workers(
    worker_clients: Vec<TestClient>,
) -> Vec<tokio::task::JoinHandle<()>> {
    worker_clients
        .into_iter()
        .map(|mut worker_client| {
            let rt = shared_bench_runtime();
            rt.spawn(async move {
                loop {
                    let Ok(frame) = worker_client.recv_frame_bytes_without_timeout().await else {
                        break;
                    };

                    if let Some(req) = try_parse_rpc_request_frame_parts(&frame) {
                        let resp_frame =
                            build_rpc_response_frame_bytes(req.correlation_id, req.body);
                        let _ = worker_client.send_frame_bytes(resp_frame).await;
                    }
                }
            })
        })
        .collect()
}

pub(crate) fn spawn_rpc_ws_workers(
    worker_clients: Vec<TestWebSocketClient>,
) -> Vec<tokio::task::JoinHandle<()>> {
    worker_clients
        .into_iter()
        .map(|mut worker_client| {
            let rt = shared_bench_runtime();
            rt.spawn(async move {
                loop {
                    let Ok(frame) = worker_client.recv_frame_bytes_without_timeout().await else {
                        break;
                    };

                    if let Some(req) = try_parse_rpc_request_frame_parts(&frame) {
                        let resp_frame =
                            build_rpc_response_frame_bytes(req.correlation_id, req.body);
                        let _ = worker_client.send_frame_bytes(resp_frame).await;
                    }
                }
            })
        })
        .collect()
}

pub(crate) fn spawn_rpc_ws_requesters(
    clients: Vec<TestWebSocketClient>,
    request_frames: Vec<Vec<NetworkRequestFrame>>,
    response_timeout_ms: u64,
) -> (
    Vec<RpcRequesterDriver>,
    std_mpsc::Receiver<Result<usize, String>>,
) {
    let (completion_tx, completion_rx) = std_mpsc::channel();
    let drivers = clients
        .into_iter()
        .zip(request_frames)
        .enumerate()
        .map(|(requester_id, (mut client, frames))| {
            let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
            let completion_tx = completion_tx.clone();
            let rt = shared_bench_runtime();
            let handle = rt.spawn(async move {
                while let Some(request_index) = command_rx.recv().await {
                    let result = tokio::time::timeout(
                        Duration::from_millis(response_timeout_ms),
                        try_request_until_worker_response_ws(&mut client, &frames[request_index]),
                    )
                    .await
                    .map_err(|_| format!("requester {requester_id} rpc response timeout"))
                    .and_then(|inner| {
                        inner
                            .map(|()| requester_id)
                            .map_err(|error| format!("requester {requester_id}: {error}"))
                    });

                    if completion_tx.send(result).is_err() {
                        break;
                    }
                }
                let _ = client.close().await;
            });
            RpcRequesterDriver { command_tx, handle }
        })
        .collect();
    drop(completion_tx);

    (drivers, completion_rx)
}

pub(crate) fn request_all_multiclient_ws(
    drivers: &[RpcRequesterDriver],
    completion_rx: &std_mpsc::Receiver<Result<usize, String>>,
    request_index: usize,
    response_timeout_ms: u64,
) {
    for driver in drivers {
        driver
            .command_tx
            .send(request_index)
            .expect("requester driver is running");
    }

    for _ in 0..drivers.len() {
        match completion_rx.recv_timeout(Duration::from_millis(response_timeout_ms)) {
            Ok(Ok(_requester_id)) => {}
            Ok(Err(error)) => panic!("{error}"),
            Err(error) => panic!("multiclient rpc response timeout: {error}"),
        }
    }
}

async fn drain_pipelined_responses_tcp(
    client: &mut TestClient,
    request_frames: &[NetworkRequestFrame],
) -> Result<(), String> {
    let mut expected = expected_response_bodies(request_frames)?;
    let max_frames_to_scan = request_frames
        .len()
        .saturating_mul(4)
        .max(request_frames.len());
    let mut scanned = 0usize;

    while !expected.is_empty() {
        scanned += 1;
        if scanned > max_frames_to_scan {
            return Err(format!(
                "expected {} rpc responses, still missing {} after scanning {scanned} frames",
                request_frames.len(),
                expected.len()
            ));
        }

        let frame = client
            .recv_frame_bytes_without_timeout()
            .await
            .map_err(|error| format!("receive pipelined rpc response: {error}"))?;
        validate_pipelined_response(&frame, &mut expected)?;
    }

    Ok(())
}

async fn drain_pipelined_responses_ws(
    client: &mut TestWebSocketClient,
    request_frames: &[NetworkRequestFrame],
) -> Result<(), String> {
    let mut expected = expected_response_bodies(request_frames)?;
    let max_frames_to_scan = request_frames
        .len()
        .saturating_mul(4)
        .max(request_frames.len());
    let mut scanned = 0usize;

    while !expected.is_empty() {
        scanned += 1;
        if scanned > max_frames_to_scan {
            return Err(format!(
                "expected {} rpc responses, still missing {} after scanning {scanned} frames",
                request_frames.len(),
                expected.len()
            ));
        }

        let frame = client
            .recv_frame_bytes_without_timeout()
            .await
            .map_err(|error| format!("receive pipelined rpc response: {error}"))?;
        validate_pipelined_response(&frame, &mut expected)?;
    }

    Ok(())
}

fn expected_response_bodies(
    request_frames: &[NetworkRequestFrame],
) -> Result<HashMap<Uuid, Bytes>, String> {
    let expected = request_frames
        .iter()
        .map(|request| (request.correlation_id, request.body.clone()))
        .collect::<HashMap<_, _>>();
    if expected.len() != request_frames.len() {
        return Err("pipelined rpc request frames must have unique correlation ids".to_string());
    }
    Ok(expected)
}

fn validate_pipelined_response(
    frame: &Bytes,
    expected: &mut HashMap<Uuid, Bytes>,
) -> Result<(), String> {
    let Some(response) = try_parse_rpc_response_frame_parts(frame) else {
        return Ok(());
    };
    let Some(expected_body) = expected.remove(&response.correlation_id) else {
        return Err(format!(
            "unexpected pipelined rpc correlation id {}",
            response.correlation_id
        ));
    };
    validate_rpc_worker_response(&response, response.correlation_id, expected_body.as_ref())
}

pub(crate) async fn complete_pipelined_requests_tcp(
    client: &mut TestClient,
    request_frames: &[NetworkRequestFrame],
    response_timeout_ms: u64,
) -> Result<(), String> {
    tokio::time::timeout(Duration::from_millis(response_timeout_ms), async {
        for request_frame in request_frames {
            client
                .send_frame_bytes(request_frame.frame.clone())
                .await
                .map_err(|error| format!("send pipelined rpc request: {error}"))?;
        }
        drain_pipelined_responses_tcp(client, request_frames).await
    })
    .await
    .map_err(|_| "pipelined tcp rpc response timeout".to_string())?
}

pub(crate) async fn complete_pipelined_requests_ws(
    client: &mut TestWebSocketClient,
    request_frames: &[NetworkRequestFrame],
    response_timeout_ms: u64,
) -> Result<(), String> {
    tokio::time::timeout(Duration::from_millis(response_timeout_ms), async {
        for request_frame in request_frames {
            client
                .send_frame_bytes(request_frame.frame.clone())
                .await
                .map_err(|error| format!("send pipelined rpc request: {error}"))?;
        }
        drain_pipelined_responses_ws(client, request_frames).await
    })
    .await
    .map_err(|_| "pipelined websocket rpc response timeout".to_string())?
}
