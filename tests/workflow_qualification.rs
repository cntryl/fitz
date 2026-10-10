//! Rust-only regression and opt-in same-host performance tests.

#[path = "workflow_qualification/policy.rs"]
mod policy;

#[path = "workflow_qualification/campaign.rs"]
mod campaign;
#[path = "workflow_qualification/compiler.rs"]
mod compiler;
#[path = "workflow_qualification/evidence_tests.rs"]
mod evidence_tests;
#[path = "workflow_qualification/native_evidence.rs"]
mod native_evidence;
#[path = "workflow_qualification/queue.rs"]
mod queue;
#[path = "workflow_qualification/queue_campaign.rs"]
mod queue_campaign;
#[path = "workflow_qualification/stream.rs"]
mod stream;
#[path = "workflow_qualification/stream_campaign.rs"]
mod stream_campaign;
#[path = "workflow_qualification/support.rs"]
mod support;
#[path = "workflow_qualification/workflow_tests.rs"]
mod workflow_tests;

#[test]
#[ignore = "Requires same-host baseline, evidence directory and full Stream benchmark campaign"]
fn should_qualify_stream_same_host() -> support::TestResult {
    // Arrange
    let mut campaign = stream_campaign::StreamCampaign::new()?;
    // Act
    let result = campaign.run();
    // Assert
    result
}

#[test]
#[ignore = "Requires same-host baseline, evidence directory and twelve full Queue captures"]
fn should_qualify_queue_same_host() -> support::TestResult {
    // Arrange
    let mut campaign = queue_campaign::QueueCampaign::new()?;
    // Act
    let result = campaign.run();
    // Assert
    result
}
