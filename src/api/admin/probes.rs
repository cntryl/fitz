//! Kubernetes probe handlers (liveness, readiness, startup)

use crate::api::http::{Body, Response};
use crate::boot::Runtime;
use hyper::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyStatus {
    pub status: &'static str,
    pub checks: CheckResults,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetStatus {
    pub status: &'static str,
    pub checks: TargetCheckResults,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResults {
    pub storage: &'static str,
    pub storage_writer_lease: &'static str,
    pub domains_initialized: &'static str,
    pub domains_healthy: &'static str,
    pub auth_configuration: &'static str,
    pub startup_complete: &'static str,
    pub accepting_traffic: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetCheckResults {
    pub http_listener: &'static str,
    pub accepting_target_traffic: &'static str,
    pub data_plane_ready: &'static str,
    pub storage_writer_lease: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartupStatus {
    pub status: &'static str,
    pub startup_time_seconds: f64,
}

/// Liveness probe - is the application alive?
/// Returns 503 only after an initialized domain permanently fails by exhausting
/// supervised restarts.
pub fn handle_liveness(runtime: &Runtime) -> Response {
    let permanently_failed = runtime.has_permanently_failed_domain();
    let response = HealthStatus {
        status: if permanently_failed { "failed" } else { "ok" },
    };
    let status = if permanently_failed {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    hyper::http::Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap()
}

fn readiness_status(runtime: &Runtime) -> ReadyStatus {
    let storage_ready = runtime.is_storage_ready();
    let auth_ready = runtime.is_auth_config_ready();
    let domains_ready = runtime.are_domains_ready();
    let domains_healthy = !runtime.has_permanently_failed_domain();
    let startup_complete = runtime.is_startup_complete();
    let ready = runtime.is_ready_for_traffic();

    ReadyStatus {
        status: if ready { "ready" } else { "not_ready" },
        checks: CheckResults {
            storage: if storage_ready { "ok" } else { "not_ready" },
            // Storage is only marked ready after Midge opens successfully, which
            // includes holding the single-writer lease for the active engine.
            storage_writer_lease: if storage_ready { "ok" } else { "not_ready" },
            domains_initialized: if domains_ready { "ok" } else { "not_ready" },
            domains_healthy: if !domains_ready {
                "not_ready"
            } else if domains_healthy {
                "ok"
            } else {
                "failed"
            },
            auth_configuration: if auth_ready { "ok" } else { "not_ready" },
            startup_complete: if startup_complete { "ok" } else { "not_ready" },
            accepting_traffic: runtime.traffic_status(),
        },
    }
}

fn target_status(runtime: &Runtime) -> TargetStatus {
    let target_ready = runtime.is_accepting_traffic();
    let data_plane_ready = runtime.is_ready_for_traffic();
    let storage_ready = runtime.is_storage_ready();

    TargetStatus {
        status: if target_ready { "ready" } else { "not_ready" },
        checks: TargetCheckResults {
            http_listener: "ok",
            accepting_target_traffic: runtime.traffic_status(),
            data_plane_ready: if data_plane_ready { "ok" } else { "not_ready" },
            storage_writer_lease: if storage_ready { "ok" } else { "not_ready" },
        },
    }
}

/// Deployment-safe health probe.
///
/// This mirrors readiness so load balancers do not route traffic until the
/// broker has completed startup and can serve as the active single writer.
pub fn handle_healthz(runtime: &Runtime) -> Response {
    handle_readiness(runtime)
}

/// Readiness probe - is the application ready to accept traffic?
pub fn handle_readiness(runtime: &Runtime) -> Response {
    let response = readiness_status(runtime);

    if response.status != "ready" {
        return hyper::http::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(&response).unwrap()))
            .unwrap();
    }

    hyper::http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap()
}

/// Orchestrator target probe - is this HTTP target alive and eligible for handoff?
///
/// This intentionally does not require the Midge writer lease. Data-plane
/// readiness remains owned by `/healthz` and `/readyz`.
pub fn handle_targetz(runtime: &Runtime) -> Response {
    let response = target_status(runtime);
    let status = if response.status == "ready" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    hyper::http::Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap()
}

/// Startup probe - has the application completed startup?
pub fn handle_startup(runtime: &Runtime) -> Response {
    if !runtime.is_startup_complete() {
        let response = HealthStatus { status: "starting" };

        return hyper::http::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(&response).unwrap()))
            .unwrap();
    }

    let response = StartupStatus {
        status: "started",
        startup_time_seconds: runtime.startup_duration().as_secs_f64(),
    };

    hyper::http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap()
}

/// Legacy health check
pub fn handle_health(runtime: &Runtime) -> Response {
    handle_healthz(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_domain_setup() -> Runtime {
        Runtime::with_test_domains_for_tests()
    }

    fn mark_runtime_ready(runtime: &Runtime) {
        runtime.mark_storage_ready();
        runtime.mark_auth_config_ready();
        runtime.mark_domains_ready();
        runtime.mark_startup_complete();
    }

    fn wait_for_permanent_domain_failure(runtime: &Runtime) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while std::time::Instant::now() < deadline {
            if runtime.has_permanently_failed_domain() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        panic!("domain actors did not fail closed");
    }

    #[test]
    fn should_keep_livez_ok_before_domains_ready_even_if_domain_failed() {
        // Arrange
        let runtime = test_domain_setup();
        runtime.mark_kv_domain_permanently_failed_for_tests();

        // Act
        let response = handle_liveness(&runtime);

        // Assert
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn should_fail_livez_after_domain_actor_panic() {
        // Arrange
        let runtime = test_domain_setup();
        mark_runtime_ready(&runtime);
        runtime.panic_all_domain_actors_for_tests();
        wait_for_permanent_domain_failure(&runtime);

        // Act
        let response = handle_liveness(&runtime);

        // Assert
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(runtime.has_permanently_failed_domain());
    }

    #[test]
    fn should_fail_livez_readyz_and_healthz_after_permanent_domain_failure() {
        // Arrange
        let runtime = test_domain_setup();
        mark_runtime_ready(&runtime);
        runtime.mark_kv_domain_permanently_failed_for_tests();

        // Act
        let livez = handle_liveness(&runtime);
        let readyz = handle_readiness(&runtime);
        let healthz = handle_healthz(&runtime);

        // Assert
        assert_eq!(livez.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(readyz.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(healthz.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(runtime.has_permanently_failed_domain());
    }
}
