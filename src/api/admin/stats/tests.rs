use super::build_bounded_global_stats;
use crate::boot::Runtime;
use crate::control::admin::read_model::AdminReadModel;
use crate::control::admin::NoticeSubscription;
use crate::runtime::Router;
use std::sync::Arc;

#[test]
fn should_build_global_subscription_count_from_bounded_projection() {
    // Arrange
    let read_model = AdminReadModel::new();
    read_model.replace_notice_subscriptions(
        (0..513)
            .map(|id| {
                NoticeSubscription::snapshot(
                    1,
                    id,
                    id,
                    "acme",
                    format!("notice://acme/jobs/job-{id}"),
                    "2026-10-04T00:00:00Z",
                )
            })
            .collect(),
    );
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), read_model);
    let snapshot = runtime.admin_read_model().bounded_snapshot(None, 2);

    // Act
    let stats = build_bounded_global_stats(&runtime, &snapshot);

    // Assert
    assert!(snapshot.truncated);
    assert_eq!(stats.domains.notice.subscriptions_active, 2);
    assert_eq!(stats.broker.realms, ["acme"]);
}

#[test]
fn should_report_fresh_queue_counts_in_family_stats_without_an_intervening_list_call() {
    // Arrange
    let fixture = crate::testkit::create_domain_runtime_fixture();
    let message_id = fixture.seed_queue_dead_letter("operations", "jobs", "dispatch");
    let runtime = fixture.runtime();
    assert!(runtime
        .queue_replay_dead_letter(
            crate::runtime::routing::RouteFamily::new(1),
            "operations",
            "jobs",
            "dispatch",
            message_id.as_u64(),
        )
        .unwrap());

    // Act
    let stats = super::build_family_stats(&runtime, 1);

    // Assert
    assert_eq!(stats.domains.queue.messages_ready, 1);
    assert_eq!(stats.domains.queue.messages_dead_lettered, 0);
}
