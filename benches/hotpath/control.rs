//! Hotpath benchmarks for control service operations
//!
//! These benchmarks test the core control service primitives that are performance-critical:
//! heartbeat, metrics, config operations on the ControlService directly.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::control::service::ControlService;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

// ---------------------------------------------------------
// Shared runtime and services
// ---------------------------------------------------------
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

static CONTROL_SERVICE: OnceLock<Arc<ControlService>> = OnceLock::new();
fn control_service() -> Arc<ControlService> {
    CONTROL_SERVICE.get_or_init(|| {
        rt().block_on(async {
            Arc::new(ControlService::new())
        })
    })
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_control_heartbeat(c: &mut Criterion) {
    let service = control_service();

    c.bench_function("control_heartbeat", |b| {
        b.iter(|| {
            rt().block_on(async {
                let result = service.heartbeat("test", "bench", "node1").await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_control_metrics(c: &mut Criterion) {
    let service = control_service();

    c.bench_function("control_metrics", |b| {
        b.iter(|| {
            rt().block_on(async {
                let result = service.get_metrics("test", "bench").await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_control_health_check(c: &mut Criterion) {
    let service = control_service();

    c.bench_function("control_health_check", |b| {
        b.iter(|| {
            rt().block_on(async {
                let result = service.health_check("test", "bench").await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_control_config_get(c: &mut Criterion) {
    let service = control_service();

    c.bench_function("control_config_get", |b| {
        b.iter(|| {
            rt().block_on(async {
                let result = service.get_config("test", "bench", "some_config").await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_control_config_set(c: &mut Criterion) {
    let service = control_service();

    c.bench_function("control_config_set", |b| {
        b.iter(|| {
            rt().block_on(async {
                let config_value = serde_json::json!({"setting": "value", "enabled": true});
                let result = service.set_config("test", "bench", "some_config", config_value).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_control_status_report(c: &mut Criterion) {
    let service = control_service();

    c.bench_function("control_status_report", |b| {
        b.iter(|| {
            rt().block_on(async {
                let status = serde_json::json!({"uptime": 3600, "connections": 42, "healthy": true});
                let result = service.report_status("test", "bench", "node1", status).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

criterion_group!(
    name = hotpath_control;
    config = config::criterion_config();
    targets =
        bench_control_heartbeat,
        bench_control_metrics,
        bench_control_health_check,
        bench_control_config_get,
        bench_control_config_set,
        bench_control_status_report
);

criterion_main!(hotpath_control);