//! Shared runtime for tier4 integration benchmarks
//!
//! Use `shared_bench_runtime()` to avoid creating a new tokio Runtime per stress test
//! when running the full tier4 suite. Each test's measure block is unchanged; only setup reuses the runtime.

use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

fn bench_logs_allowed() -> bool {
    std::env::var("FITZ_BENCH_ALLOW_LOGS")
        .ok()
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(false)
}

fn init_bench_observability() {
    let _ = if bench_logs_allowed() {
        crate::observability::global::try_init_observability_with_defaults(Some("off"), Some(false))
    } else {
        crate::observability::global::try_init_bench_observability()
    };
}

/// Shared tokio Runtime for integration benchmarks. Reused across tests in the same binary.
pub static SHARED_BENCH_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    init_bench_observability();
    Runtime::new().expect("create shared bench runtime")
});

/// Returns a reference to the shared benchmark runtime. Use for tier4 TCP/WS test setup.
#[must_use]
pub fn shared_bench_runtime() -> &'static Runtime {
    &SHARED_BENCH_RUNTIME
}
