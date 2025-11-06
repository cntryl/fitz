use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::{Domain, DomainResponse};
use fitz::core::notice::{NoticeDomain, NoticeService};
use fitz::protocol::frame::build_tlv;
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

// Helper to build a DomainRequest for a given raw route and payload
fn make_request(raw: &str, payload: Vec<u8>) -> fitz::core::domain::DomainRequest {
    fitz::core::domain::DomainRequest {
        route: fitz::protocol::route::Route {
            scheme: fitz::protocol::route::Scheme::Notice,
            realm: Some("bench".to_string()),
            area: Some("hotpath".to_string()),
            resource: Some("test".to_string()),
            operation: Some("event".to_string()),
            raw: raw.to_string(),
        },
        route_str: raw.to_string(),
        payload,
        channel_id: 1,
    }
}

// Shared NoticeService instance
fn shared_service() -> Arc<std::sync::Mutex<NoticeService>> {
    Arc::new(std::sync::Mutex::new(NoticeService::new()))
}

// Shared NoticeDomain instance
fn shared_domain() -> Arc<NoticeDomain> {
    Arc::new(NoticeDomain::new())
}

// ============================================================================
// SERVICE BENCHMARKS
// ============================================================================

/// Benchmark: Subscribe to route pattern
fn bench_service_subscribe(c: &mut Criterion) {
    let svc = shared_service();
    
    c.bench_function("service_subscribe", |b| {
        b.iter(|| {
            let mut svc = svc.lock().unwrap();
            let (tx, _rx) = mpsc::channel(100);
            svc.subscribe("notice://bench/hotpath/test/*".to_string(), 1, tx)
        });
    });
}

/// Benchmark: Unsubscribe by ID
fn bench_service_unsubscribe(c: &mut Criterion) {
    c.bench_function("service_unsubscribe", |b| {
        b.iter(|| {
            let svc = shared_service();
            let mut svc = svc.lock().unwrap();
            let (tx, _rx) = mpsc::channel(100);
            let sub_id = svc.subscribe("notice://bench/hotpath/test/*".to_string(), 1, tx);
            svc.unsubscribe(sub_id)
        });
    });
}

/// Benchmark: Publish with no subscribers
fn bench_service_publish_no_subscribers(c: &mut Criterion) {
    let svc = shared_service();
    
    c.bench_function("service_publish_no_subscribers", |b| {
        b.iter(|| {
            let mut svc = svc.lock().unwrap();
            svc.publish(
                "notice://bench/hotpath/test/event",
                Some("msg-123"),
                b"benchmark payload",
            )
        });
    });
}

/// Benchmark: Publish with 1 subscriber
fn bench_service_publish_one_subscriber(c: &mut Criterion) {
    let svc = shared_service();
    let (tx, _rx) = mpsc::channel(1000);
    {
        let mut svc = svc.lock().unwrap();
        svc.subscribe("notice://bench/hotpath/test/*".to_string(), 1, tx);
    }
    
    c.bench_function("service_publish_one_subscriber", |b| {
        b.iter(|| {
            let mut svc = svc.lock().unwrap();
            svc.publish(
                "notice://bench/hotpath/test/event",
                Some("msg-123"),
                b"benchmark payload",
            )
        });
    });
}

/// Benchmark: Publish with 10 subscribers
fn bench_service_publish_ten_subscribers(c: &mut Criterion) {
    let svc = shared_service();
    
    // Add 10 subscribers with different patterns
    for i in 0..10 {
        let (tx, _rx) = mpsc::channel(1000);
        let mut svc = svc.lock().unwrap();
        let pattern = if i < 5 {
            "notice://bench/hotpath/test/*".to_string()
        } else {
            format!("notice://bench/hotpath/test/event{}", i)
        };
        svc.subscribe(pattern, i as u32, tx);
    }
    
    c.bench_function("service_publish_ten_subscribers", |b| {
        b.iter(|| {
            let mut svc = svc.lock().unwrap();
            svc.publish(
                "notice://bench/hotpath/test/event",
                Some("msg-123"),
                b"benchmark payload",
            )
        });
    });
}

/// Benchmark: Publish with wildcard matching (5 matching out of 100 total)
fn bench_service_publish_wildcard_matching(c: &mut Criterion) {
    let svc = shared_service();
    
    // Add 100 subscribers, only 5 will match
    for i in 0..100 {
        let (tx, _rx) = mpsc::channel(1000);
        let mut svc = svc.lock().unwrap();
        let pattern = if i < 5 {
            "notice://bench/hotpath/*".to_string()
        } else {
            format!("notice://other/area{}/resource/op", i)
        };
        svc.subscribe(pattern, i as u32, tx);
    }
    
    c.bench_function("service_publish_wildcard_matching", |b| {
        b.iter(|| {
            let mut svc = svc.lock().unwrap();
            svc.publish(
                "notice://bench/hotpath/test/event",
                Some("msg-123"),
                b"benchmark payload",
            )
        });
    });
}

/// Benchmark: Subscribe, publish, and unsubscribe cycle
fn bench_service_subscribe_publish_unsubscribe_cycle(c: &mut Criterion) {
    let svc = shared_service();
    
    c.bench_function("service_subscribe_publish_unsubscribe_cycle", |b| {
        b.iter(|| {
            let (tx, _rx) = mpsc::channel(1000);
            let mut svc = svc.lock().unwrap();
            let sub_id = svc.subscribe("notice://bench/hotpath/test/*".to_string(), 1, tx);
            let _ = svc.publish(
                "notice://bench/hotpath/test/event",
                Some("msg-123"),
                b"benchmark payload",
            );
            svc.unsubscribe(sub_id)
        });
    });
}

// ============================================================================
// HANDLER BENCHMARKS
// ============================================================================

/// Benchmark: Subscribe via handler
fn bench_handler_subscribe(c: &mut Criterion) {
    let domain = shared_domain();
    
    c.bench_function("handler_subscribe", |b| {
        b.iter(|| {
            let domain = domain.clone();
            rt().block_on(async move {
                let mut payload = Vec::new();
                build_tlv(TAG_SUBSCRIBE, b"notice://bench/hotpath/test/*", &mut payload);
                
                let req = make_request("notice://bench/hotpath/test/*", payload);
                (&*domain).handle(req).await
            })
        });
    });
}

/// Benchmark: Publish via handler (no subscribers)
fn bench_handler_publish_no_subscribers(c: &mut Criterion) {
    let domain = shared_domain();
    
    c.bench_function("handler_publish_no_subscribers", |b| {
        b.iter(|| {
            let domain = domain.clone();
            rt().block_on(async move {
                let mut payload = Vec::new();
                build_tlv(TAG_BODY, b"benchmark payload", &mut payload);
                build_tlv(TAG_ID, b"msg-123", &mut payload);
                
                let req = make_request("notice://bench/hotpath/test/event", payload);
                (&*domain).handle(req).await
            })
        });
    });
}

/// Benchmark: Publish via handler (with 1 subscriber)
fn bench_handler_publish_one_subscriber(c: &mut Criterion) {
    let domain = shared_domain();
    
    // Subscribe first
    rt().block_on(async {
        let mut payload = Vec::new();
        build_tlv(TAG_SUBSCRIBE, b"notice://bench/hotpath/test/*", &mut payload);
        let req = make_request("notice://bench/hotpath/test/*", payload);
        let _ = (&*domain).handle(req).await;
    });
    
    c.bench_function("handler_publish_one_subscriber", |b| {
        b.iter(|| {
            let domain = domain.clone();
            rt().block_on(async move {
                let mut payload = Vec::new();
                build_tlv(TAG_BODY, b"benchmark payload", &mut payload);
                build_tlv(TAG_ID, b"msg-123", &mut payload);
                
                let req = make_request("notice://bench/hotpath/test/event", payload);
                (&*domain).handle(req).await
            })
        });
    });
}

/// Benchmark: Subscribe, publish, unsubscribe via handler
fn bench_handler_subscribe_publish_unsubscribe(c: &mut Criterion) {
    let domain = shared_domain();
    
    c.bench_function("handler_subscribe_publish_unsubscribe", |b| {
        b.iter(|| {
            let domain = domain.clone();
            rt().block_on(async move {
                // Subscribe
                let mut sub_payload = Vec::new();
                build_tlv(TAG_SUBSCRIBE, b"notice://bench/hotpath/test/*", &mut sub_payload);
                let sub_req = make_request("notice://bench/hotpath/test/*", sub_payload);
                let sub_resp = (&*domain).handle(sub_req).await;
                
                // Extract subscription ID
                let sub_id = if let DomainResponse::Frame(frame) = sub_resp {
                    fitz::protocol::frame::find_tlv(frame.as_ref(), TAG_ID)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .map(|s| s.to_string())
                } else {
                    None
                };
                
                // Publish
                let mut pub_payload = Vec::new();
                build_tlv(TAG_BODY, b"benchmark payload", &mut pub_payload);
                let pub_req = make_request("notice://bench/hotpath/test/event", pub_payload);
                let _ = (&*domain).handle(pub_req).await;
                
                // Unsubscribe
                if let Some(sub_id) = sub_id {
                    let mut unsub_payload = Vec::new();
                    build_tlv(TAG_UNSUBSCRIBE, sub_id.as_bytes(), &mut unsub_payload);
                    let unsub_req = make_request("notice://bench/hotpath/test/*", unsub_payload);
                    let _ = (&*domain).handle(unsub_req).await;
                }
            })
        });
    });
}

criterion_group! {
    name = hotpath_notice;
    config = config::criterion_config();
    targets =
        // Service benchmarks
        bench_service_subscribe,
        bench_service_unsubscribe,
        bench_service_publish_no_subscribers,
        bench_service_publish_one_subscriber,
        bench_service_publish_ten_subscribers,
        bench_service_publish_wildcard_matching,
        bench_service_subscribe_publish_unsubscribe_cycle,
        // Handler benchmarks
        bench_handler_subscribe,
        bench_handler_publish_no_subscribers,
        bench_handler_publish_one_subscriber,
        bench_handler_subscribe_publish_unsubscribe,
}

criterion_main!(hotpath_notice);
