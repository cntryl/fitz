// Compiles the Tier 5 capacity-boundary decision under the test harness; the
// harness-free bench targets never build its unit tests.
#[path = "../benches/stress_support/capacity_boundary.rs"]
#[allow(dead_code)]
mod capacity_boundary;
