use clap::Parser;
use fitz::testkit::{TestServer, TestWebSocketClient};
use serde::Serialize;
use std::ffi::c_void;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;

#[derive(Parser, Debug)]
#[command(author, version, about = "Measure Fitz production characteristics")]
pub struct Args {
    #[arg(long, default_value_t = 1500)]
    pub single_duration_ms: u64,

    #[arg(long, default_value_t = 1500)]
    pub scaling_duration_ms: u64,

    #[arg(long, default_value = "1,4,8,16")]
    pub client_counts: String,

    #[arg(long, default_value_t = 256)]
    pub resource_samples: usize,

    #[arg(long, default_value_t = 64)]
    pub connection_samples: usize,

    #[arg(long, default_value = "target/production_characterization")]
    pub output_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyStats {
    pub unit: String,
    pub samples: usize,
    pub meaningful_ops: u64,
    pub elapsed_ms: f64,
    pub ops_per_s: f64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub max_us: f64,
    pub errors: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScalingPoint {
    pub dimension: String,
    pub count: usize,
    pub stats: LatencyStats,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryCost {
    pub resource: String,
    pub sample_count: usize,
    pub bytes_total_delta: i64,
    pub bytes_per_resource: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioReport {
    pub name: String,
    pub single_client_ws: LatencyStats,
    pub scaling_curve_ws: Vec<ScalingPoint>,
    pub suspected_cliff_at: Option<usize>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DomainReport {
    pub domain: String,
    pub single_client_ws: LatencyStats,
    pub scaling_curve_ws: Vec<ScalingPoint>,
    pub suspected_cliff_at: Option<usize>,
    pub additional_scenarios: Vec<ScenarioReport>,
    pub resource_memory: MemoryCost,
    pub idle_connection_bytes_per_client: i64,
    pub notes: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ProductionReport {
    pub generated_at: String,
    pub transport: String,
    pub single_duration_ms: u64,
    pub scaling_duration_ms: u64,
    pub idle_connection_samples: usize,
    pub resource_samples: usize,
    pub idle_ws_connection_bytes_per_client: i64,
    pub domains: Vec<DomainReport>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct ClientRun {
    pub latencies_us: Vec<u64>,
    pub errors: usize,
}

#[cfg(windows)]
#[repr(C)]
struct ProcessMemoryCounters {
    cb: u32,
    page_fault_count: u32,
    peak_working_set_size: usize,
    working_set_size: usize,
    quota_peak_paged_pool_usage: usize,
    quota_paged_pool_usage: usize,
    quota_peak_non_paged_pool_usage: usize,
    quota_non_paged_pool_usage: usize,
    pagefile_usage: usize,
    peak_pagefile_usage: usize,
}

#[cfg(windows)]
#[link(name = "Kernel32")]
unsafe extern "system" {
    fn GetCurrentProcess() -> *mut c_void;
}

#[cfg(windows)]
#[link(name = "Psapi")]
unsafe extern "system" {
    fn GetProcessMemoryInfo(
        process: *mut c_void,
        counters: *mut ProcessMemoryCounters,
        cb: u32,
    ) -> i32;
}

pub fn parse_bench_args() -> Args {
    let filtered_args: Vec<_> = std::env::args_os().filter(|arg| arg != "--bench").collect();
    Args::parse_from(filtered_args)
}

pub fn configure_characterization_env() {
    if std::env::var_os("FITZ_LOG_LEVEL").is_none() {
        std::env::set_var("FITZ_LOG_LEVEL", "error");
    }

    if std::env::var_os("OTEL_ENABLED").is_none() {
        std::env::set_var("OTEL_ENABLED", "false");
    }
}

pub fn parse_counts(value: &str) -> Result<Vec<usize>, String> {
    let counts: Vec<usize> = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<usize>()
                .map_err(|error| format!("invalid count '{part}': {error}"))
        })
        .collect::<Result<_, _>>()?;

    if counts.is_empty() {
        return Err("client_counts must contain at least one value".to_string());
    }

    Ok(counts)
}

fn percentile(values: &[u64], fraction: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    if values.len() == 1 {
        return values[0] as f64;
    }

    let position = (values.len() - 1) as f64 * fraction;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        return values[lower] as f64;
    }

    let weight = position - lower as f64;
    values[lower] as f64 + (values[upper] as f64 - values[lower] as f64) * weight
}

pub fn compute_stats(
    unit: &str,
    elapsed: Duration,
    latencies_us: Vec<u64>,
    meaningful_ops_per_sample: u64,
    errors: usize,
) -> LatencyStats {
    let mut sorted = latencies_us;
    sorted.sort_unstable();
    let sample_count = sorted.len();
    let meaningful_ops = sample_count as u64 * meaningful_ops_per_sample;
    let mean_us = if sample_count == 0 {
        0.0
    } else {
        sorted.iter().map(|value| *value as f64).sum::<f64>() / sample_count as f64
    };

    LatencyStats {
        unit: unit.to_string(),
        samples: sample_count,
        meaningful_ops,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        ops_per_s: if elapsed.is_zero() {
            0.0
        } else {
            meaningful_ops as f64 / elapsed.as_secs_f64()
        },
        mean_us,
        p50_us: percentile(&sorted, 0.50),
        p95_us: percentile(&sorted, 0.95),
        p99_us: percentile(&sorted, 0.99),
        max_us: sorted.last().copied().unwrap_or_default() as f64,
        errors,
    }
}

pub fn detect_cliff(points: &[ScalingPoint]) -> Option<usize> {
    for window in points.windows(2) {
        let previous = &window[0].stats;
        let current = &window[1].stats;

        let throughput_regressed = current.ops_per_s < previous.ops_per_s * 0.90;
        let tail_spike =
            current.p99_us > current.p50_us * 3.0 && current.p99_us > previous.p99_us * 1.25;
        if throughput_regressed || tail_spike {
            return Some(window[1].count);
        }
    }

    None
}

#[cfg(windows)]
fn current_working_set_bytes() -> Result<u64, String> {
    let process = unsafe { GetCurrentProcess() };
    let mut counters = ProcessMemoryCounters {
        cb: std::mem::size_of::<ProcessMemoryCounters>() as u32,
        page_fault_count: 0,
        peak_working_set_size: 0,
        working_set_size: 0,
        quota_peak_paged_pool_usage: 0,
        quota_paged_pool_usage: 0,
        quota_peak_non_paged_pool_usage: 0,
        quota_non_paged_pool_usage: 0,
        pagefile_usage: 0,
        peak_pagefile_usage: 0,
    };

    let success = unsafe {
        GetProcessMemoryInfo(
            process,
            &mut counters,
            std::mem::size_of::<ProcessMemoryCounters>() as u32,
        )
    };

    if success == 0 {
        return Err("GetProcessMemoryInfo failed".to_string());
    }

    Ok(counters.working_set_size as u64)
}

#[cfg(not(windows))]
fn current_working_set_bytes() -> Result<u64, String> {
    Err("working set measurement is only implemented for Windows in this bench".to_string())
}

pub fn stable_working_set_bytes() -> Result<u64, String> {
    let mut samples = Vec::new();
    for _ in 0..3 {
        samples.push(current_working_set_bytes()?);
        thread::sleep(Duration::from_millis(25));
    }
    samples.sort_unstable();
    Ok(samples[samples.len() / 2])
}

pub fn delta_per_unit(before: u64, after: u64, count: usize) -> MemoryCost {
    let delta = after as i64 - before as i64;
    let per_resource = if count == 0 { 0 } else { delta / count as i64 };
    MemoryCost {
        resource: String::new(),
        sample_count: count,
        bytes_total_delta: delta,
        bytes_per_resource: per_resource,
    }
}

pub fn measure_idle_ws_connection_cost(
    runtime: &Runtime,
    sample_count: usize,
) -> Result<i64, String> {
    let server = runtime
        .block_on(TestServer::start())
        .map_err(|error| error.to_string())?;

    thread::sleep(Duration::from_millis(100));
    let before = stable_working_set_bytes()?;

    let mut clients = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        clients.push(
            runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .map_err(|error| error.to_string())?,
        );
    }
    runtime
        .block_on(server.wait_for_session_count(sample_count))
        .map_err(|error| error.to_string())?;
    thread::sleep(Duration::from_millis(150));
    let after = stable_working_set_bytes()?;

    for client in &mut clients {
        let _ = runtime.block_on(client.close());
    }

    Ok((after as i64 - before as i64) / sample_count as i64)
}

pub fn write_report(
    output_dir: &Path,
    report: &ProductionReport,
    domain: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(output_dir)?;
    let output_json = output_dir.join(format!("{}.json", domain));
    let output_markdown = output_dir.join(format!("{}.md", domain));

    fs::write(&output_json, serde_json::to_string_pretty(report)?)?;
    fs::write(&output_markdown, render_markdown(report))?;

    println!("Wrote {}", output_json.display());
    println!("Wrote {}", output_markdown.display());
    Ok(())
}

fn render_markdown(report: &ProductionReport) -> String {
    let mut output = String::new();
    output.push_str("# Production Characterization\n\n");
    output.push_str(&format!("- generated_at: {}\n", report.generated_at));
    output.push_str(&format!("- transport: {}\n", report.transport));
    output.push_str(&format!(
        "- idle_ws_connection_bytes_per_client: {}\n\n",
        report.idle_ws_connection_bytes_per_client
    ));

    for domain in &report.domains {
        output.push_str(&format!("## {}\n\n", domain.domain.to_uppercase()));
        render_scenario_markdown(
            &mut output,
            None,
            &domain.single_client_ws,
            &domain.scaling_curve_ws,
            domain.suspected_cliff_at,
            &[],
        );
        for scenario in &domain.additional_scenarios {
            render_scenario_markdown(
                &mut output,
                Some(&scenario.name),
                &scenario.single_client_ws,
                &scenario.scaling_curve_ws,
                scenario.suspected_cliff_at,
                &scenario.notes,
            );
        }
        output.push_str(&format!(
            "- {} bytes_per_resource: {} (delta: {})\n",
            domain.resource_memory.resource,
            domain.resource_memory.bytes_per_resource,
            domain.resource_memory.bytes_total_delta
        ));
        output.push_str(&format!(
            "- idle_connection_bytes_per_client: {}\n",
            domain.idle_connection_bytes_per_client
        ));
        if !domain.notes.is_empty() {
            output.push_str("- notes:\n");
            for note in &domain.notes {
                output.push_str(&format!("  - {}\n", note));
            }
        }
        output.push('\n');
    }

    output
}

fn render_scenario_markdown(
    output: &mut String,
    title: Option<&str>,
    single_client_ws: &LatencyStats,
    scaling_curve_ws: &[ScalingPoint],
    suspected_cliff_at: Option<usize>,
    notes: &[String],
) {
    if let Some(title) = title {
        output.push_str(&format!("### {}\n\n", title));
        output.push_str("#### Single Client\n\n");
    } else {
        output.push_str("### Single Client\n\n");
    }

    output
        .push_str("| unit | ops_per_s | p50_us | p95_us | p99_us | mean_us | max_us | errors |\n");
    output.push_str("|---|---:|---:|---:|---:|---:|---:|---:|\n");
    output.push_str(&format!(
        "| {} | {:.0} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} | {} |\n\n",
        single_client_ws.unit,
        single_client_ws.ops_per_s,
        single_client_ws.p50_us,
        single_client_ws.p95_us,
        single_client_ws.p99_us,
        single_client_ws.mean_us,
        single_client_ws.max_us,
        single_client_ws.errors,
    ));

    if title.is_some() {
        output.push_str("#### Scaling\n\n");
    } else {
        output.push_str("### Scaling\n\n");
    }
    output.push_str("| dimension | count | ops_per_s | p50_us | p95_us | p99_us | errors |\n");
    output.push_str("|---|---:|---:|---:|---:|---:|---:|\n");
    for point in scaling_curve_ws {
        output.push_str(&format!(
            "| {} | {} | {:.0} | {:.1} | {:.1} | {:.1} | {} |\n",
            point.dimension,
            point.count,
            point.stats.ops_per_s,
            point.stats.p50_us,
            point.stats.p95_us,
            point.stats.p99_us,
            point.stats.errors,
        ));
    }
    output.push('\n');
    output.push_str(&format!(
        "- suspected_cliff_at: {}\n",
        suspected_cliff_at
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none observed".to_string())
    ));
    if !notes.is_empty() {
        output.push_str("- scenario_notes:\n");
        for note in notes {
            output.push_str(&format!("  - {}\n", note));
        }
    }
    output.push('\n');
}
