use super::*;

pub(crate) fn collect_queue_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("queue") {
        return;
    }

    for queue in runtime.queue_list_queues(options.realm.as_deref()) {
        let route_family = queue.family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&queue.realm),
            Some(&queue.area),
            Some(&queue.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "messages_ready".to_string(),
            queue.messages_ready.to_string(),
        );
        metadata.insert(
            "messages_delayed".to_string(),
            queue.messages_delayed.to_string(),
        );
        metadata.insert(
            "messages_inflight".to_string(),
            queue.messages_inflight.to_string(),
        );
        metadata.insert(
            "messages_dead_lettered".to_string(),
            queue.messages_dead_lettered.to_string(),
        );
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "queue:resource:{}:{}:{}:{}",
                    route_family, queue.realm, queue.area, queue.resource
                ),
                domain: "queue".to_string(),
                kind: "resource".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(queue.realm.clone()),
                area: Some(queue.area.clone()),
                resource: Some(queue.resource.clone()),
                operation: None,
                title: queue.resource.clone(),
                summary: format!(
                    "{} ready, {} delayed, {} inflight, {} dead-lettered",
                    queue.messages_ready,
                    queue.messages_delayed,
                    queue.messages_inflight,
                    queue.messages_dead_lettered
                ),
                health: Some(
                    if queue.messages_dead_lettered > 0 {
                        "failing"
                    } else if queue.messages_ready + queue.messages_delayed > 0 {
                        "backlogged"
                    } else {
                        "healthy"
                    }
                    .to_string(),
                ),
                href: resource_href("queue", &queue.realm, &queue.area, &queue.resource),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("realm", queue.realm),
                ("area", queue.area),
                ("resource", queue.resource),
            ],
        ));
    }

    for inflight in runtime.queue_list_inflight(options.realm.as_deref()) {
        let route_family = inflight.family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&inflight.realm),
            Some(&inflight.area),
            Some(&inflight.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("message_id".to_string(), inflight.message_id.to_string());
        metadata.insert("session_id".to_string(), inflight.session_id.clone());
        metadata.insert("attempts".to_string(), inflight.attempts.to_string());
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "queue:inflight:{}:{}:{}:{}:{}",
                    route_family,
                    inflight.realm,
                    inflight.area,
                    inflight.resource,
                    inflight.message_id
                ),
                domain: "queue".to_string(),
                kind: "inflight_message".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(inflight.realm.clone()),
                area: Some(inflight.area.clone()),
                resource: Some(inflight.resource.clone()),
                operation: None,
                title: format!("Inflight message {}", inflight.message_id),
                summary: format!(
                    "Session {}, expires {}",
                    inflight.session_id, inflight.expires_at
                ),
                health: Some("inflight".to_string()),
                href: resource_href("queue", &inflight.realm, &inflight.area, &inflight.resource),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("message_id", inflight.message_id.to_string()),
                ("session_id", inflight.session_id),
                ("realm", inflight.realm),
                ("area", inflight.area),
                ("resource", inflight.resource),
                ("inflight_token", inflight.inflight_token),
            ],
        ));
    }

    for message in runtime.queue_list_dead_letters(options.realm.as_deref()) {
        let route_family = message.family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&message.realm),
            Some(&message.area),
            Some(&message.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("message_id".to_string(), message.message_id.to_string());
        metadata.insert("attempts".to_string(), message.attempts.to_string());
        metadata.insert("reason".to_string(), message.reason.clone());
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "queue:dead-letter:{}:{}:{}:{}:{}",
                    route_family, message.realm, message.area, message.resource, message.message_id
                ),
                domain: "queue".to_string(),
                kind: "dead_letter".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(message.realm.clone()),
                area: Some(message.area.clone()),
                resource: Some(message.resource.clone()),
                operation: None,
                title: format!("Dead-letter message {}", message.message_id),
                summary: format!(
                    "{} attempt(s), reason: {}",
                    message.attempts, message.reason
                ),
                health: Some("failing".to_string()),
                href: resource_href("queue", &message.realm, &message.area, &message.resource),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("message_id", message.message_id.to_string()),
                ("reason", message.reason),
                ("realm", message.realm),
                ("area", message.area),
                ("resource", message.resource),
            ],
        ));
    }
}

pub(crate) fn collect_schedule_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("schedule") {
        return;
    }

    for schedule in runtime.schedule_list_schedules(options.realm.as_deref()) {
        let route_family = schedule.route_family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&schedule.realm),
            Some(&schedule.area),
            Some(&schedule.resource),
            Some(&schedule.operation),
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("cron".to_string(), schedule.cron.clone());
        metadata.insert("next_run".to_string(), schedule.next_run.clone());
        metadata.insert(
            "executions_total".to_string(),
            schedule.executions_total.to_string(),
        );
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "schedule:{}:{}:{}:{}",
                    schedule.realm, schedule.area, schedule.resource, schedule.operation
                ),
                domain: "schedule".to_string(),
                kind: "schedule".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(schedule.realm.clone()),
                area: Some(schedule.area.clone()),
                resource: Some(schedule.resource.clone()),
                operation: Some(schedule.operation.clone()),
                title: format!("{}/{}", schedule.resource, schedule.operation),
                summary: format!("{}; next run {}", schedule.cron, schedule.next_run),
                health: Some(
                    if schedule.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    }
                    .to_string(),
                ),
                href: resource_href(
                    "schedule",
                    &schedule.realm,
                    &schedule.area,
                    &schedule.resource,
                ),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("realm", schedule.realm),
                ("area", schedule.area),
                ("resource", schedule.resource),
                ("operation", schedule.operation),
                ("cron", schedule.cron),
                ("next_run", schedule.next_run),
            ],
        ));
    }
}

pub(crate) fn collect_lease_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("lease") {
        return;
    }

    for lease in runtime.lease_list_leases(options.realm.as_deref()) {
        let route_family = lease.route_family.to_string();
        if !options.matches_scope(
            Some(&route_family),
            Some(&lease.realm),
            Some(&lease.area),
            Some(&lease.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "owner_session_id".to_string(),
            lease.owner_session_id.clone(),
        );
        metadata.insert("fencing_token".to_string(), lease.fencing_token.to_string());
        metadata.insert("renewals".to_string(), lease.renewals.to_string());
        candidates.push(candidate(
            AdminSearchResult {
                id: format!("lease:{}:{}:{}", lease.realm, lease.area, lease.resource),
                domain: "lease".to_string(),
                kind: "lease".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(lease.realm.clone()),
                area: Some(lease.area.clone()),
                resource: Some(lease.resource.clone()),
                operation: None,
                title: lease.resource.clone(),
                summary: format!(
                    "Owned by {} until {}",
                    lease.owner_session_id, lease.expires_at
                ),
                health: Some("owned".to_string()),
                href: resource_href("lease", &lease.realm, &lease.area, &lease.resource),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("realm", lease.realm),
                ("area", lease.area),
                ("resource", lease.resource),
                ("owner_session_id", lease.owner_session_id),
                ("fencing_token", lease.fencing_token.to_string()),
            ],
        ));
    }
}

pub(crate) fn collect_notice_candidates(
    runtime: &Runtime,
    options: &SearchOptions,
    candidates: &mut Vec<Candidate>,
) {
    if !options.matches_domain("notice") {
        return;
    }

    for subscription in runtime.notice_list_subscriptions(options.realm.as_deref(), None) {
        let route_family = subscription.route_family.to_string();
        let parsed = route_triplet(&subscription.pattern);
        if !options.matches_scope(
            Some(&route_family),
            Some(&subscription.realm),
            parsed.map(|route| route.area),
            parsed.map(|route| route.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "subscription_id".to_string(),
            subscription.subscription_id.to_string(),
        );
        metadata.insert("session_id".to_string(), subscription.session_id.clone());
        metadata.insert(
            "notifications_received".to_string(),
            subscription.notifications_received.to_string(),
        );
        candidates.push(candidate(
            AdminSearchResult {
                id: format!(
                    "notice:subscription:{}:{}",
                    subscription.session_id, subscription.subscription_id
                ),
                domain: "notice".to_string(),
                kind: "subscription".to_string(),
                route_family: Some(route_family.clone()),
                realm: Some(subscription.realm.clone()),
                area: parsed.map(|route| route.area.to_string()),
                resource: parsed.map(|route| route.resource.to_string()),
                operation: None,
                title: subscription.pattern.clone(),
                summary: format!(
                    "Session {} active since {}",
                    subscription.session_id, subscription.created_at
                ),
                health: Some("live".to_string()),
                href: parsed
                    .map(|route| resource_href("notice", route.realm, route.area, route.resource))
                    .unwrap_or_else(|| "/notice".to_string()),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("realm", subscription.realm),
                ("pattern", subscription.pattern),
                ("session_id", subscription.session_id),
                ("subscription_id", subscription.subscription_id.to_string()),
            ],
        ));
    }

    for route in runtime.notice_list_routes(options.realm.as_deref()) {
        let route_family = route.route_family.to_string();
        let parsed = route_triplet(&route.route);
        if !options.matches_scope(
            Some(&route_family),
            parsed.map(|value| value.realm),
            parsed.map(|value| value.area),
            parsed.map(|value| value.resource),
            None,
        ) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        metadata.insert("subscribers".to_string(), route.subscribers.to_string());
        metadata.insert(
            "publishes_total".to_string(),
            route.publishes_total.to_string(),
        );
        candidates.push(candidate(
            AdminSearchResult {
                id: format!("notice:route:{}", route.route),
                domain: "notice".to_string(),
                kind: "route".to_string(),
                route_family: Some(route_family.clone()),
                realm: parsed.map(|value| value.realm.to_string()),
                area: parsed.map(|value| value.area.to_string()),
                resource: parsed.map(|value| value.resource.to_string()),
                operation: None,
                title: route.route.clone(),
                summary: format!(
                    "{} subscriber(s), {} publishes",
                    route.subscribers, route.publishes_total
                ),
                health: Some(
                    if route.subscribers > 0 {
                        "observed"
                    } else {
                        "quiet"
                    }
                    .to_string(),
                ),
                href: parsed
                    .map(|value| resource_href("notice", value.realm, value.area, value.resource))
                    .unwrap_or_else(|| "/notice".to_string()),
                matched_fields: Vec::new(),
                metadata,
            },
            vec![
                ("route_family", route_family),
                ("route", route.route),
                ("subscribers", route.subscribers.to_string()),
                ("publishes_total", route.publishes_total.to_string()),
            ],
        ));
    }
}
