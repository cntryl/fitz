use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::Domain;
use fitz::core::rpc::RpcDomain;
use fitz::protocol::tags::*;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

// Shared Tokio runtime
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

// ============================================================================
// SERVICE BENCHMARKS (DEPRECATED - Moved to dispatch-only model)
// ============================================================================
//
// Note: The following service benchmarks (allocate_inbox, subscribe_handler, etc.)
// were testing internal service methods that are no longer exposed to benchmarks.
// The RPC domain now operates in dispatch-only mode where all operations come
// through the Domain::handle() trait method.
//
// If you need to benchmark these operations, they should be benchmarked as part
// of the dispatch flow through handle(), not as standalone service methods.

// /// Benchmark: Allocate inbox (cryptographically secure UUID generation)
// fn bench_service_allocate_inbox(c: &mut Criterion) {
//     let rt = rt();
//     let domain = Arc::new(RpcDomain::new());
//
//     c.bench_function("service_allocate_inbox", |b| {
//         b.iter(|| {
//             rt.block_on(async {
//                 domain.allocate_inbox(1).await
//             })
//         });
//     });
// }

// ... other service benchmarks removed for brevity ...

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

    let request = fitz::core::domain::DomainContext {
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
        route_family: 0,
        sender: None,
    };

    c.bench_function("domain_parse_tlv_minimal", |b| {
        b.iter(|| rt.block_on(async { domain.handle(request.clone()).await }));
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

    let request = fitz::core::domain::DomainContext {
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
        route_family: 0,
        sender: None,
    };

    c.bench_function("domain_parse_tlv_full", |b| {
        b.iter(|| rt.block_on(async { domain.handle(request.clone()).await }));
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

    let request = fitz::core::domain::DomainContext {
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
        route_family: 0,
        sender: None,
    };

    c.bench_function("domain_parse_tlv_streaming", |b| {
        b.iter(|| rt.block_on(async { domain.handle(request.clone()).await }));
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

    let request = fitz::core::domain::DomainContext {
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
        route_family: 0,
        sender: None,
    };

    c.bench_function("domain_build_subscribe_response", |b| {
        b.iter(|| rt.block_on(async { domain.handle(request.clone()).await }));
    });
}

/// Benchmark: Build TLV error response
fn bench_domain_build_error_response(c: &mut Criterion) {
    let rt = rt();
    let domain = Arc::new(RpcDomain::new());

    // Malformed request (missing required fields)
    let payload = vec![0xFF, 0x10]; // Invalid TLV

    let request = fitz::core::domain::DomainContext {
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
        route_family: 0,
        sender: None,
    };

    c.bench_function("domain_build_error_response", |b| {
        b.iter(|| rt.block_on(async { domain.handle(request.clone()).await }));
    });
}

// ============================================================================
// CLIENT BENCHMARKS (Would require mock engine)
// ============================================================================

// Note: Client benchmarks are commented out as they require a full engine setup
// Uncomment and implement create_mock_engine() and create_mock_rpc_domain() for full client benchmarking

/*
/// Benchmark: Create RPC client (allocate inbox + subscribe)
fn bench_client_new(c: &mut Criterion) {
    let rt = rt();

    c.bench_function("client_new", |b| {
        b.iter(|| {
            rt.block_on(async {
                let engine = create_mock_engine();
                let rpc_domain = create_mock_rpc_domain();
                RpcClient::new(engine, rpc_domain, 1).await
            })
        });
    });
}

/// Benchmark: Call unary (publish + await single response)
fn bench_client_call_unary(c: &mut Criterion) {
    let rt = rt();
    let engine = create_mock_engine();
    let rpc_domain = create_mock_rpc_domain();
    let client = rt.block_on(async {
        RpcClient::new(engine, rpc_domain, 1).await.unwrap()
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
        // Service benchmarks (removed - now dispatch-only)

        // Domain handler benchmarks (TLV)
        bench_domain_parse_tlv_minimal,
        bench_domain_parse_tlv_full,
        bench_domain_parse_tlv_streaming,
        bench_domain_build_subscribe_response,
        bench_domain_build_error_response,
}

criterion_main!(hotpath_rpc);
