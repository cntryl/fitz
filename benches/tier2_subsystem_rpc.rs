use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::rpc::rpc_route_actor::RpcRouteActor;
use fitz::domains::rpc::protocol::{RpcMessage, RpcRequest, RpcResponse};
use uuid::Uuid;
use fitz::prelude::Actor;
use fitz::benchkit::create_bench_rpc_context;
use fitz::protocol::mux::Mux;
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};

#[path = "config.rs"]
mod config;

/// Encode an RPC request into TLV format
fn encode_rpc_request(correlation_id: &str, route: &str, body: &[u8]) -> Vec<u8> {
    let mut encoder = TlvEncoder::with_capacity(512);
    
    // Simplified RPC request encoding:
    // Tag 1: correlation_id
    // Tag 2: route
    // Tag 3: reply_route
    // Tag 4: body
    encoder.encode(MessageType::new(1), correlation_id.as_bytes());
    encoder.encode(MessageType::new(2), route.as_bytes());
    encoder.encode(MessageType::new(3), b"inbox://session/1");
    encoder.encode(MessageType::new(4), body);
    
    encoder.finish().to_vec()
}

/// Encode an RPC response into TLV format
fn encode_rpc_response(correlation_id: &str, seq: u32, stream_end: bool, body: &[u8]) -> Vec<u8> {
    let mut encoder = TlvEncoder::with_capacity(512);
    
    // Simplified RPC response encoding:
    // Tag 10: correlation_id
    // Tag 11: seq
    // Tag 12: stream_end
    // Tag 13: body
    encoder.encode(MessageType::new(10), correlation_id.as_bytes());
    encoder.encode(MessageType::new(11), &seq.to_le_bytes());
    encoder.encode(MessageType::new(12), &[stream_end as u8]);
    encoder.encode(MessageType::new(13), body);
    
    encoder.finish().to_vec()
}

fn bench_subsystem_request_dispatch(c: &mut Criterion) {
    let sizes = [16usize, 64usize, 256usize, 1024usize];
    let worker_counts = [1usize, 4usize, 16usize];
    
    let mut group = c.benchmark_group("subsystem_rpc_dispatch");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        for &worker_count in &worker_counts {
            // Precompute TLV-encoded requests outside the hot path
            let requests: Vec<Vec<u8>> = (0..256)
                .map(|i| {
                    let body = vec![0u8; size];
                    encode_rpc_request(
                        &format!("req-{}", i),
                        "rpc://realm/service/operation",
                        &body,
                    )
                })
                .collect();

            // Setup actor with workers
            let mut actor = RpcRouteActor::new(RouteFamily::new(1));
            let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");
            
            for i in 0..worker_count {
                let worker_addr = RouteAddress::new(
                    RouteFamily::new(1),
                    Route::new(format!("worker://realm/service/worker{}", i)),
                );
                actor.receive(
                    RpcMessage::Subscribe {
                        worker_addr: worker_addr.clone(),
                    },
                    &mut ctx,
                );
            }

            let mut mux = Mux::new(256);
            let decoder = TlvDecoder::new();

            let name = format!("dispatch_{}B_{}workers", size, worker_count);
            group.throughput(Throughput::Elements(1));
            
            group.bench_function(&name, |b| {
                let mut idx = 0usize;
                b.iter(|| {
                    let data = &requests[idx % requests.len()];
                    
                    // Parse TLV
                            let mut correlation_id: Option<Uuid> = None;
                    let mut route = String::new();
                    let mut reply_route = String::new();
                    let mut body = Bytes::from(vec![]);
                    
                    for res in decoder.iter(black_box(data)) {
                        let (mt, slice) = res.unwrap();
                        
                        // Route through mux
                        if let Ok((_cref, _grant)) = mux.route_grant(mt, slice) {
                            match mt.as_u16() {
                                1 => {
                                    let id_str = String::from_utf8_lossy(slice);
                                    correlation_id = Some(Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::new_v4()));
                                },
                                2 => route = String::from_utf8_lossy(slice).to_string(),
                                3 => reply_route = String::from_utf8_lossy(slice).to_string(),
                                4 => body = Bytes::from(slice.to_vec()),
                                _ => {}
                            }
                        }
                    }
                    
                    // Dispatch to actor
                    let cid = correlation_id.unwrap_or_else(Uuid::new_v4);
                    let req = RpcRequest {
                        family_id: RouteFamily::new(1),
                        correlation_id: cid,
                        route: Route::new(route),
                        reply_route: Route::new(reply_route),
                        body,
                    };
                    actor.receive(RpcMessage::Request(req), &mut ctx);
                    
                    idx += 1;
                })
            });
        }
    }

    group.finish();
}

fn bench_subsystem_response_routing(c: &mut Criterion) {
    let sizes = [16usize, 64usize, 256usize, 1024usize];
    
    let mut group = c.benchmark_group("subsystem_rpc_response");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        // Setup actor with worker and dispatched request
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");
        
        let worker_addr = RouteAddress::new(
            RouteFamily::new(1),
            Route::new("worker://realm/service/worker1"),
        );
        actor.receive(
            RpcMessage::Subscribe {
                worker_addr: worker_addr.clone(),
            },
            &mut ctx,
        );

        // Dispatch initial request to establish lease
        let initial_cid = Uuid::new_v4();
        let initial_req = RpcRequest {
            family_id: RouteFamily::new(1),
            correlation_id: initial_cid,
            route: Route::new("rpc://realm/service/operation"),
            reply_route: Route::new("inbox://session/1"),
            body: Bytes::from(vec![0u8; size]),
        };
        actor.receive(RpcMessage::Request(initial_req), &mut ctx);

        // Precompute TLV-encoded responses
        let body = vec![0u8; size];
        let response_data = encode_rpc_response(&initial_cid.to_string(), 0, true, &body);

        let mut mux = Mux::new(256);
        let decoder = TlvDecoder::new();

        let name = format!("response_routing_{}B", size);
        group.throughput(Throughput::Elements(1));
        
        group.bench_function(&name, |b| {
            b.iter(|| {
                // Parse TLV
                let mut correlation_id: Option<Uuid> = None;
                let mut seq = 0u64;
                let mut stream_end = false;
                let mut body = Bytes::from(vec![]);
                
                for res in decoder.iter(black_box(&response_data)) {
                    let (mt, slice) = res.unwrap();
                    
                    // Route through mux
                    if let Ok((_cref, _grant)) = mux.route_grant(mt, slice) {
                        match mt.as_u16() {
                            10 => correlation_id = Some(Uuid::parse_str(&String::from_utf8_lossy(slice)).unwrap_or_else(|_| Uuid::new_v4())),
                            11 => {
                                if slice.len() >= 4 {
                                    seq = u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]) as u64;
                                }
                            }
                            12 => stream_end = slice.first().copied().unwrap_or(0) != 0,
                            13 => body = Bytes::from(slice.to_vec()),
                            _ => {}
                        }
                    }
                }
                
                // Handle response
                let cid = correlation_id.unwrap_or_else(Uuid::new_v4);
                let resp = RpcResponse {
                    correlation_id: cid,
                    seq,
                    stream_end,
                    body,
                };
                actor.receive(RpcMessage::Response(resp), &mut ctx);
                
                // Re-establish lease for next iteration
                let req = RpcRequest {
                    family_id: RouteFamily::new(1),
                    correlation_id: initial_cid,
                    route: Route::new("rpc://realm/service/operation"),
                    reply_route: Route::new("inbox://session/1"),
                    body: Bytes::from(vec![0u8; size]),
                };
                actor.receive(RpcMessage::Request(req), &mut ctx);
            })
        });
    }

    group.finish();
}

fn bench_subsystem_streaming_response(c: &mut Criterion) {
    let chunk_sizes = [64usize, 256usize, 1024usize];
    let chunk_counts = [4usize, 16usize, 64usize];
    
    let mut group = c.benchmark_group("subsystem_rpc_streaming");
    group.sampling_mode(SamplingMode::Flat);

    for &chunk_size in &chunk_sizes {
        for &chunk_count in &chunk_counts {
            // Setup actor with worker
            let mut actor = RpcRouteActor::new(RouteFamily::new(1));
            let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");
            
            let worker_addr = RouteAddress::new(
                RouteFamily::new(1),
                Route::new("worker://realm/service/worker1"),
            );
            actor.receive(
                RpcMessage::Subscribe {
                    worker_addr: worker_addr.clone(),
                },
                &mut ctx,
            );

            // Dispatch initial request
            let initial_cid = Uuid::new_v4();
            let initial_req = RpcRequest {
                family_id: RouteFamily::new(1),
                correlation_id: initial_cid,
                route: Route::new("rpc://realm/service/operation"),
                reply_route: Route::new("inbox://session/1"),
                body: Bytes::from(vec![0u8; 64]),
            };
            actor.receive(RpcMessage::Request(initial_req), &mut ctx);

            // Precompute streaming response chunks
            let responses: Vec<Vec<u8>> = (0..chunk_count)
                .map(|i| {
                    let body = vec![0u8; chunk_size];
                    let is_last = i == chunk_count - 1;
                    encode_rpc_response(&initial_cid.to_string(), i as u32, is_last, &body)
                })
                .collect();

            let mut mux = Mux::new(256);
            let decoder = TlvDecoder::new();

            let name = format!("streaming_{}x{}B", chunk_count, chunk_size);
            group.throughput(Throughput::Elements(chunk_count as u64));
            
            group.bench_function(&name, |b| {
                b.iter(|| {
                    for data in &responses {
                        // Parse TLV
                        let mut correlation_id: Option<Uuid> = None;
                        let mut seq = 0u64;
                        let mut stream_end = false;
                        let mut body = Bytes::from(vec![]);
                        
                        for res in decoder.iter(black_box(data)) {
                            let (mt, slice) = res.unwrap();
                            
                            if let Ok((_cref, _grant)) = mux.route_grant(mt, slice) {
                                        match mt.as_u16() {
                                    10 => correlation_id = Some(Uuid::parse_str(&String::from_utf8_lossy(slice)).unwrap_or_else(|_| Uuid::new_v4())),
                                    11 => {
                                        if slice.len() >= 4 {
                                            seq = u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]) as u64;
                                        }
                                    }
                                    12 => stream_end = slice.first().copied().unwrap_or(0) != 0,
                                    13 => body = Bytes::from(slice.to_vec()),
                                    _ => {}
                                }
                            }
                        }
                        
                        // Handle response chunk
                        let cid = correlation_id.unwrap_or_else(Uuid::new_v4);
                        let resp = RpcResponse {
                            correlation_id: cid,
                            seq,
                            stream_end,
                            body: body.clone(),
                        };
                        actor.receive(RpcMessage::Response(resp), &mut ctx);
                    }
                    
                    // Re-establish request for next iteration
                    let req = RpcRequest {
                        family_id: RouteFamily::new(1),
                        correlation_id: initial_cid,
                        route: Route::new("rpc://realm/service/operation"),
                        reply_route: Route::new("inbox://session/1"),
                        body: Bytes::from(vec![0u8; 64]),
                    };
                    actor.receive(RpcMessage::Request(req), &mut ctx);
                })
            });
        }
    }

    group.finish();
}

fn bench_subsystem_backpressure(c: &mut Criterion) {
    let request_counts = [10usize, 100usize, 500usize, 1000usize];
    
    let mut group = c.benchmark_group("subsystem_rpc_backpressure");
    group.sampling_mode(SamplingMode::Flat);

    for &request_count in &request_counts {
        // Precompute TLV-encoded requests
        let requests: Vec<Vec<u8>> = (0..request_count)
            .map(|_| {
                let body = vec![0u8; 64];
                encode_rpc_request(
                    &Uuid::new_v4().to_string(),
                    "rpc://realm/service/operation",
                    &body,
                )
            })
            .collect();

        // Setup actor with custom capacity (no workers = all requests queue)
        let mut actor = RpcRouteActor::with_capacity(RouteFamily::new(1), 1000);
        let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");

        let mut mux = Mux::new(request_count);
        let decoder = TlvDecoder::new();

        let name = format!("queue_{}_requests", request_count);
        group.throughput(Throughput::Elements(request_count as u64));
        
        group.bench_function(&name, |b| {
            b.iter(|| {
                for data in &requests {
                    // Parse TLV
                    let mut correlation_id: Option<Uuid> = None;
                    let mut route = String::new();
                    let mut reply_route = String::new();
                    let mut body = Bytes::from(vec![]);
                    
                    for res in decoder.iter(black_box(data)) {
                        let (mt, slice) = res.unwrap();
                        
                        if let Ok((_cref, _grant)) = mux.route_grant(mt, slice) {
                            match mt.as_u16() {
                                1 => correlation_id = Some(Uuid::parse_str(&String::from_utf8_lossy(slice)).unwrap_or_else(|_| Uuid::new_v4())),
                                2 => route = String::from_utf8_lossy(slice).to_string(),
                                3 => reply_route = String::from_utf8_lossy(slice).to_string(),
                                4 => body = Bytes::from(slice.to_vec()),
                                _ => {}
                            }
                        }
                    }
                    
                    // Enqueue request
                    let cid = correlation_id.unwrap_or_else(Uuid::new_v4);
                    let req = RpcRequest {
                        family_id: RouteFamily::new(1),
                        correlation_id: cid,
                        route: Route::new(route),
                        reply_route: Route::new(reply_route),
                        body,
                    };
                    actor.receive(RpcMessage::Request(req), &mut ctx);
                }
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_subsystem_request_dispatch,
        bench_subsystem_response_routing,
        bench_subsystem_streaming_response,
        bench_subsystem_backpressure
}
criterion_main!(benches);
