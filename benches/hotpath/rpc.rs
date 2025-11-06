use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::Domain;
use fitz::core::rpc::RpcDomain;
use fitz::protocol::tags::*;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

#[path = "../config.rs"]
mod config;

// Shared Tokio runtime
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

// ============================================================================
// SERVICE BENCHMARKS
// ============================================================================

/// Benchmark: Allocate inbox (cryptographically secure UUID generation)
fn bench_service_allocate_inbox(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());
    
    c.bench_function("service_allocate_inbox", |b| {
        b.iter(|| {
            rt.block_on(async {
                domain.allocate_inbox(1).await
            })
        });
    });
}

/// Benchmark: Subscribe to handler route
fn bench_service_subscribe_handler(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());
    
    c.bench_function("service_subscribe_handler", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, _rx) = mpsc::channel(100);
                domain.subscribe_handler(
                    "rpc://acme/auth/user/create".to_string(),
                    1,
                    tx
                ).await
            })
        });
    });
}

/// Benchmark: Subscribe to inbox (with ownership check)
fn bench_service_subscribe_inbox(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());
    
    c.bench_function("service_subscribe_inbox", |b| {
        b.iter(|| {
            rt.block_on(async {
                let inbox = domain.allocate_inbox(1).await;
                let (tx, _rx) = mpsc::channel(100);
                domain.subscribe_inbox(inbox, 1, tx).await
            })
        });
    });
}

/// Benchmark: Unsubscribe (cleanup)
fn bench_service_unsubscribe(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());
    
    c.bench_function("service_unsubscribe", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, _rx) = mpsc::channel(100);
                let sub_id = domain.subscribe_handler(
                    "rpc://acme/auth/user/create".to_string(),
                    1,
                    tx
                ).await;
                domain.unsubscribe(sub_id).await
            })
        });
    });
}

/// Benchmark: Match handler routes (critical path for request routing)
fn bench_service_match_handlers(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());
    let service = domain.get_service();
    
    // Pre-populate with 100 handler subscriptions
    rt.block_on(async {
        for i in 0..100 {
            let (tx, _rx) = mpsc::channel(100);
            let mut svc = service.write().await;
            svc.subscribe_handler(
                format!("rpc://acme/service{}/resource/operation", i),
                i,
                tx
            );
        }
    });
    
    c.bench_function("service_match_handlers", |b| {
        b.iter(|| {
            rt.block_on(async {
                let svc = service.read().await;
                svc.matching_handlers("rpc://acme/service50/resource/operation")
            })
        });
    });
}

/// Benchmark: Check inbox authorization (critical for reply security)
fn bench_service_can_publish_to_inbox(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());
    let service = domain.get_service();
    
    // Setup: Register active request
    rt.block_on(async {
        let mut svc = service.write().await;
        svc.register_request(
            "req-123".to_string(),
            "rpc://acme/auth/user/create".to_string(),
            "inbox://test-inbox-uuid".to_string()
        );
    });
    
    c.bench_function("service_can_publish_to_inbox", |b| {
        b.iter(|| {
            rt.block_on(async {
                let svc = service.read().await;
                svc.can_publish_to_inbox("inbox://test-inbox-uuid", "req-123")
            })
        });
    });
}

/// Benchmark: Cleanup channel (disconnect scenario)
fn bench_service_cleanup_channel(c: &mut Criterion) {
    let rt = rt();
    
    c.bench_function("service_cleanup_channel", |b| {
        b.iter(|| {
            let domain = Arc::new(RpcDomain::new());
            rt.block_on(async {
                // Setup: Create subscriptions and inboxes
                let (tx, _rx) = mpsc::channel(100);
                let inbox = domain.allocate_inbox(1).await;
                let _ = domain.subscribe_inbox(inbox, 1, tx.clone()).await;
                let _ = domain.subscribe_handler(
                    "rpc://test/svc/op".to_string(),
                    1,
                    tx
                ).await;
                
                // Cleanup
                domain.cleanup_channel(1).await;
            })
        });
    });
}

// ============================================================================
// DOMAIN HANDLER BENCHMARKS (TLV Processing)
// ============================================================================

/// Benchmark: Parse TLV payload with minimal fields (hot path)
fn bench_domain_parse_tlv_minimal(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());
    
    // Build minimal TLV: route only
    let mut payload = Vec::new();
    payload.push(TAG_ROUTE);
    let route = b"rpc://acme/auth/user/create";
    payload.push(route.len() as u8);
    payload.extend_from_slice(route);
    
    let request = fitz::core::domain::DomainRequest {
        route: fitz::protocol::route::Route {
            scheme: fitz::protocol::route::Scheme::Rpc,
            realm: Some("acme".to_string()),
            area: Some("auth".to_string()),
            resource: Some("user".to_string()),
            operation: Some("create".to_string()),
            raw: "rpc://acme/auth/user/create".to_string(),
        },
        route_str: "rpc://acme/auth/user/create".to_string(),
        payload: payload.clone(),
        channel_id: 1,
    };
    
    c.bench_function("domain_parse_tlv_minimal", |b| {
        b.iter(|| {
            rt.block_on(async {
                domain.handle(request.clone()).await
            })
        });
    });
}

/// Benchmark: Parse TLV payload with all fields (request with reply route)
fn bench_domain_parse_tlv_full(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());
    
    // Build full TLV: route + id + body + reply_route
    let mut payload = Vec::new();
    
    // TAG_ROUTE
    payload.push(TAG_ROUTE);
    let route = b"rpc://acme/auth/user/create";
    payload.push(route.len() as u8);
    payload.extend_from_slice(route);
    
    // TAG_ID (correlation ID)
    payload.push(TAG_ID);
    let corr_id = b"req-12345678";
    payload.push(corr_id.len() as u8);
    payload.extend_from_slice(corr_id);
    
    // TAG_BODY
    payload.push(TAG_BODY);
    let body = b"{\"username\":\"alice\",\"email\":\"alice@example.com\"}";
    payload.push(body.len() as u8);
    payload.extend_from_slice(body);
    
    // TAG_ROUTE_REPLY
    payload.push(TAG_ROUTE_REPLY);
    let reply_route = b"inbox://a1b2c3d4-e5f6-7890-abcd-ef1234567890";
    payload.push(reply_route.len() as u8);
    payload.extend_from_slice(reply_route);
    
    let request = fitz::core::domain::DomainRequest {
        route: fitz::protocol::route::Route {
            scheme: fitz::protocol::route::Scheme::Rpc,
            realm: Some("acme".to_string()),
            area: Some("auth".to_string()),
            resource: Some("user".to_string()),
            operation: Some("create".to_string()),
            raw: "rpc://acme/auth/user/create".to_string(),
        },
        route_str: "rpc://acme/auth/user/create".to_string(),
        payload: payload.clone(),
        channel_id: 1,
    };
    
    c.bench_function("domain_parse_tlv_full", |b| {
        b.iter(|| {
            rt.block_on(async {
                domain.handle(request.clone()).await
            })
        });
    });
}

/// Benchmark: Parse TLV with streaming fields (seq + stream_end)
fn bench_domain_parse_tlv_streaming(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());
    
    // Build streaming TLV
    let mut payload = Vec::new();
    
    // TAG_ROUTE (inbox route for reply)
    payload.push(TAG_ROUTE);
    let route = b"inbox://test-inbox-uuid";
    payload.push(route.len() as u8);
    payload.extend_from_slice(route);
    
    // TAG_ID
    payload.push(TAG_ID);
    let corr_id = b"req-123";
    payload.push(corr_id.len() as u8);
    payload.extend_from_slice(corr_id);
    
    // TAG_SEQ
    payload.push(TAG_SEQ);
    payload.push(8);
    payload.extend_from_slice(&5u64.to_be_bytes());
    
    // TAG_BODY
    payload.push(TAG_BODY);
    let body = b"chunk data";
    payload.push(body.len() as u8);
    payload.extend_from_slice(body);
    
    // TAG_STREAM_END
    payload.push(TAG_STREAM_END);
    payload.push(0);
    
    let request = fitz::core::domain::DomainRequest {
        route: fitz::protocol::route::Route {
            scheme: fitz::protocol::route::Scheme::Rpc,
            realm: None,
            area: None,
            resource: None,
            operation: None,
            raw: "inbox://test-inbox-uuid".to_string(),
        },
        route_str: "inbox://test-inbox-uuid".to_string(),
        payload: payload.clone(),
        channel_id: 1,
    };
    
    c.bench_function("domain_parse_tlv_streaming", |b| {
        b.iter(|| {
            rt.block_on(async {
                domain.handle(request.clone()).await
            })
        });
    });
}

/// Benchmark: Build TLV response (subscribe ack)
fn bench_domain_build_subscribe_response(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());
    
    // Subscribe request
    let mut payload = Vec::new();
    payload.push(TAG_SUBSCRIBE);
    payload.push(0);
    payload.push(TAG_ROUTE);
    let route = b"rpc://acme/auth/user/create";
    payload.push(route.len() as u8);
    payload.extend_from_slice(route);
    
    let request = fitz::core::domain::DomainRequest {
        route: fitz::protocol::route::Route {
            scheme: fitz::protocol::route::Scheme::Rpc,
            realm: Some("acme".to_string()),
            area: Some("auth".to_string()),
            resource: Some("user".to_string()),
            operation: Some("create".to_string()),
            raw: "rpc://acme/auth/user/create".to_string(),
        },
        route_str: "rpc://acme/auth/user/create".to_string(),
        payload: payload.clone(),
        channel_id: 1,
    };
    
    c.bench_function("domain_build_subscribe_response", |b| {
        b.iter(|| {
            rt.block_on(async {
                domain.handle(request.clone()).await
            })
        });
    });
}

/// Benchmark: Build TLV error response
fn bench_domain_build_error_response(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());
    
    // Malformed request (missing required fields)
    let payload = vec![0xFF, 0x10]; // Invalid TLV
    
    let request = fitz::core::domain::DomainRequest {
        route: fitz::protocol::route::Route {
            scheme: fitz::protocol::route::Scheme::Rpc,
            realm: None,
            area: None,
            resource: None,
            operation: None,
            raw: "rpc://invalid".to_string(),
        },
        route_str: "rpc://invalid".to_string(),
        payload,
        channel_id: 1,
    };
    
    c.bench_function("domain_build_error_response", |b| {
        b.iter(|| {
            rt.block_on(async {
                domain.handle(request.clone()).await
            })
        });
    });
}

// ============================================================================
// CLIENT BENCHMARKS (Would require mock engine)
// ============================================================================

// Note: Client benchmarks are commented out as they require a full engine setup
// Uncomment and implement create_mock_engine() for full client benchmarking

/*
/// Benchmark: Create RPC client (allocate inbox + subscribe)
fn bench_client_new(c: &mut Criterion) {
    let rt = rt();
    
    c.bench_function("client_new", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = create_mock_engine();
                RpcClient::new(engine, 1).await
            })
        });
    });
}

/// Benchmark: Call unary (publish + await single response)
fn bench_client_call_unary(c: &mut Criterion) {
    let rt = rt();
    let engine = create_mock_engine();
    let client = rt.block_on(async {
        RpcClient::new(engine, 1).await.unwrap()
    });
    
    c.bench_function("client_call_unary", |b| {
        b.iter(|| {
            rt.block_on(async {
                client.call_unary(
                    "rpc://acme/auth/user/get",
                    "req-123",
                    b"{\"user_id\":\"alice\"}"
                ).await
            })
        });
    });
}
*/

// ============================================================================
// CRITERION CONFIGURATION
// ============================================================================

criterion_group! {
    name = hotpath_rpc;
    config = config::criterion_config();
    targets =
        // Service benchmarks
        bench_service_allocate_inbox,
        bench_service_subscribe_handler,
        bench_service_subscribe_inbox,
        bench_service_unsubscribe,
        bench_service_match_handlers,
        bench_service_can_publish_to_inbox,
        bench_service_cleanup_channel,
        
        // Domain handler benchmarks (TLV)
        bench_domain_parse_tlv_minimal,
        bench_domain_parse_tlv_full,
        bench_domain_parse_tlv_streaming,
        bench_domain_build_subscribe_response,
        bench_domain_build_error_response,
}

criterion_main!(hotpath_rpc);
