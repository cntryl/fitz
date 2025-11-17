// Moved from benches/hotpath/rpc.rs — now a subsystem/service-level bench
use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::SubSender;
use fitz::core::rpc::RpcService;
use fitz::routing::DEFAULT_RF;
use std::sync::OnceLock;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

#[path = "../config.rs"]
mod config;

#[allow(dead_code)]
static RT: OnceLock<Runtime> = OnceLock::new();
#[allow(dead_code)]
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

fn rpc_service() -> RpcService {
    RpcService::new()
}

fn bench_rpc_allocate_inbox(c: &mut Criterion) {
    let mut svc = rpc_service();

    c.bench_function("rpc_allocate_inbox", |b| {
        b.iter(|| {
            let _ = svc.allocate_inbox(1);
        })
    });
}

fn bench_rpc_subscribe_and_publish(c: &mut Criterion) {
    let mut svc = rpc_service();
    let (tx, _rx) = mpsc::channel(1);
    let sender: SubSender = tx;

    c.bench_function("rpc_subscribe_inbox_and_match", |b| {
        b.iter(|| {
            // allocate an inbox and subscribe
            let inbox = svc.allocate_inbox(1);
            let _ = svc
                .subscribe_inbox(DEFAULT_RF, inbox.clone(), 1, sender.clone())
                .is_ok();
            // matching handlers is a hotpath, exercise it
            let matches = svc.matching_inbox_subscribers(DEFAULT_RF, &inbox);
            criterion::black_box(matches);
        })
    });
}

criterion_group!(
    name = subsystem_rpc_service;
    config = config::criterion_config();
    targets = bench_rpc_allocate_inbox, bench_rpc_subscribe_and_publish
);
criterion_main!(subsystem_rpc_service);
