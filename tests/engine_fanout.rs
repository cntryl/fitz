use crossbeam_channel::unbounded;
use std::sync::Arc;
use tokio::sync::mpsc;

use fitz::authz::{PermissionGrants, SessionAuth};
use fitz::core::engine::{
    Engine, EngineConnectionRegistry, EngineEvent, EngineHandle, NUM_SHARDS,
};
use fitz::core::registry::DomainRegistry;
use fitz::protocol::frame::build_frame;
use fitz::protocol::tags::{FRAME_DAT, TAG_BODY, TAG_ROUTE, TAG_SUBSCRIBE};

// Helper to build a subscribe frame
fn make_subscribe_frame(channel_id: u32, route: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    // TAG_ROUTE (2-byte length per new TLV spec)
    payload.push(TAG_ROUTE);
    let rlen = route.len() as u16;
    payload.push((rlen >> 8) as u8);
    payload.push(rlen as u8);
    payload.extend_from_slice(route.as_bytes());
    // TAG_SUBSCRIBE (empty value, 0 length encoded with two bytes)
    payload.push(TAG_SUBSCRIBE);
    payload.push(0);
    payload.push(0);
    build_frame(FRAME_DAT, 0, channel_id, &payload)
}

// Helper to build a publish frame (route + body)
fn make_publish_frame(channel_id: u32, route: &str, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    // TAG_ROUTE (2-byte length)
    payload.push(TAG_ROUTE);
    let rlen = route.len() as u16;
    payload.push((rlen >> 8) as u8);
    payload.push(rlen as u8);
    payload.extend_from_slice(route.as_bytes());
    // TAG_BODY (2-byte length)
    payload.push(TAG_BODY);
    let blen = body.len() as u16;
    payload.push((blen >> 8) as u8);
    payload.push(blen as u8);
    payload.extend_from_slice(body);
    build_frame(FRAME_DAT, 0, channel_id, &payload)
}

#[test]
fn should_fanout_notice_publish_to_all_subscribers() {
    // Arrange
    let route = "notice://dev/area/topic/update";
    let registry = Arc::new(EngineConnectionRegistry::new());
    let domains = Arc::new(DomainRegistry::new());

    // Create shards (single inbox per shard)
    let mut shard_handles: Vec<EngineHandle> = Vec::new();
    for _ in 0..NUM_SHARDS {
        let (tx, rx) = unbounded::<EngineEvent>();
        let handle = EngineHandle::new(tx.clone(), Arc::clone(&domains), Arc::clone(&registry));
        shard_handles.push(handle);
        // Spawn engine run loop in a background thread
        let engine = Engine::new(rx, Arc::clone(&registry), Arc::clone(&domains));
        std::thread::spawn(move || engine.run());
    }

    let pool = fitz::core::engine::EnginePool::new(shard_handles.clone().try_into().unwrap());
    let shard = pool.get_handle("dev");

    // Sessions for three connections (two subscribers + one publisher)
    let grants = PermissionGrants::from_scopes("dev", &["*".to_string()]);
    let session_template = |subject: &str| SessionAuth {
        subject: subject.to_string(),
        route_family: "dev".to_string(),
        scopes: vec!["*".to_string()],
        grants: grants.clone(),
    };

    // Outbound channels capture frames for assertions
    let (sub1_tx, mut sub1_rx) = mpsc::channel::<Arc<Vec<u8>>>(16);
    let (sub2_tx, mut sub2_rx) = mpsc::channel::<Arc<Vec<u8>>>(16);
    let (pub_tx, _pub_rx) = mpsc::channel::<Arc<Vec<u8>>>(16);

    let conn_sub1 = 1u64;
    let conn_sub2 = 2u64;
    let conn_pub = 3u64;

    shard.register_session(conn_sub1, session_template("sub1"));
    shard.register_session(conn_sub2, session_template("sub2"));
    shard.register_session(conn_pub, session_template("pub"));

    shard.register_connection(conn_sub1, sub1_tx);
    shard.register_connection(conn_sub2, sub2_tx);
    shard.register_connection(conn_pub, pub_tx);

    // Build and send subscribe frames (channel ids distinct)
    let frame_sub1 = make_subscribe_frame(10, route);
    let frame_sub2 = make_subscribe_frame(20, route);
    shard.on_frame(conn_sub1, frame_sub1);
    shard.on_frame(conn_sub2, frame_sub2);

    // Act
    let publish_body = b"hello-world";
    let frame_pub = make_publish_frame(30, route, publish_body);
    shard.on_frame(conn_pub, frame_pub);

    // Assert
    // Drain subscriber outbound queues; each should receive at least one frame containing body bytes
    let mut received_sub1 = Vec::new();
    let mut received_sub2 = Vec::new();
    // Allow brief time for engine threads to process
    std::thread::sleep(std::time::Duration::from_millis(50));
    while let Ok(f) = sub1_rx.try_recv() {
        received_sub1.push(f);
    }
    while let Ok(f) = sub2_rx.try_recv() {
        received_sub2.push(f);
    }

    assert!(
        !received_sub1.is_empty(),
        "subscriber 1 did not receive any frames"
    );
    assert!(
        !received_sub2.is_empty(),
        "subscriber 2 did not receive any frames"
    );

    // Notification frames now include only body (route is implicit via subscription)
    // Header (10) + (tag + 2 len bytes) for body + body bytes
    let expected_min = 10 /*header*/ + 3 + publish_body.len();
    assert!(
        received_sub1.iter().any(|f| f.len() >= expected_min && f.windows(publish_body.len()).any(|w| w == publish_body)),
        "no sufficiently sized notification frame for subscriber 1"
    );
    assert!(
        received_sub2.iter().any(|f| f.len() >= expected_min && f.windows(publish_body.len()).any(|w| w == publish_body)),
        "no sufficiently sized notification frame for subscriber 2"
    );
}
