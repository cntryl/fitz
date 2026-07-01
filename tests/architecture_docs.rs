const ARCHITECTURE_DOC: &str = include_str!("../docs/development/architecture.md");

fn section_between<'a>(contents: &'a str, start: &str, end: &str) -> &'a str {
    let start_index = contents.find(start).expect("section start");
    let section = &contents[start_index..];
    let end_index = section.find(end).expect("section end");
    &section[..end_index]
}

#[test]
fn should_keep_architecture_storage_schema_aligned_with_domain_contracts() {
    // Arrange
    let storage_schema = section_between(
        ARCHITECTURE_DOC,
        "**Key-value schema for each domain:**",
        "## Wire Protocol Implementation",
    );

    // Act
    let documents_lease_storage = storage_schema.contains("- **Lease:**");
    let documents_schedule_storage = storage_schema.contains("- **Schedule:**")
        && storage_schema.contains("persisted definitions")
        && storage_schema.contains("next-fire state")
        && storage_schema.contains("pending fire claims");

    // Assert
    assert!(!documents_lease_storage);
    assert!(documents_schedule_storage);
}

#[test]
fn should_describe_route_family_realm_separation_as_isolation_axes() {
    // Arrange
    let overview = section_between(ARCHITECTURE_DOC, "## Overview", "## System Architecture");

    // Act
    let documents_hard_route_family_isolation =
        overview.contains("hard broker isolation by `RouteFamily`");
    let documents_app_visible_realm_namespace =
        overview.contains("app-visible namespace by `realm`");
    let documents_no_axis_substitution = ARCHITECTURE_DOC.contains(
        "`realm` and `RouteFamily` are separate axes and must never be inferred, aliased, substituted, or used as fallback values for each other",
    );

    // Assert
    assert!(documents_hard_route_family_isolation);
    assert!(documents_app_visible_realm_namespace);
    assert!(documents_no_axis_substitution);
}

#[test]
fn should_document_cleanup_retry_without_session_recovery() {
    // Arrange
    let session_section = section_between(
        ARCHITECTURE_DOC,
        "### Session Recovery Model",
        "### Layer 3: Runtime",
    );

    // Act
    let documents_cleanup_retry =
        session_section.contains("cleanup retry tickets complete cleanup dispatch only");
    let rejects_session_recovery = session_section.contains(
        "never restore sessions, ownership, subscriptions, transactions, workers, leases, or inflight state",
    );

    // Assert
    assert!(documents_cleanup_retry);
    assert!(rejects_session_recovery);
}

#[test]
fn should_document_queue_actor_mailbox_contract() {
    // Arrange
    let queue_section = section_between(ARCHITECTURE_DOC, "#### Queue", "#### Notice");

    // Act
    let documents_actor_owned_live_paths =
        queue_section.contains("delivery, cleanup, runtime sweeps, live admin refresh");
    let documents_dlq_command_replies = queue_section
        .contains("dead-letter replay and purge use explicit `Runtime::queue_*_dead_letter` command/reply messages through the actor mailbox");
    let documents_mailbox_cleanup =
        queue_section.contains("disconnect cleanup is enqueued to the Queue actor mailbox");

    // Assert
    assert!(documents_actor_owned_live_paths);
    assert!(documents_dlq_command_replies);
    assert!(documents_mailbox_cleanup);
}
