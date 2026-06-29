use super::super::*;

pub async fn notice_delivery_observations(
    runtime: Arc<Runtime>,
    family: u64,
    realm: Option<String>,
    area: Option<String>,
    resource: Option<String>,
    query: Option<String>,
    limit: usize,
) -> Result<Response, Infallible> {
    let routes = runtime.notice_list_routes(realm.as_deref());
    let route_stats: HashMap<_, _> = routes
        .into_iter()
        .filter(|route| route.route_family == family)
        .map(|route| ((route.route_family, route.route.clone()), route))
        .collect();
    let observations = runtime
        .notice_list_subscriptions(realm.as_deref(), None)
        .into_iter()
        .filter(|subscription| subscription.route_family == family)
        .filter_map(|subscription| {
            let parsed = parse_flexible_route(&subscription.pattern);
            if area
                .as_ref()
                .is_none_or(|value| parsed.as_ref().map(|parts| &parts.area) == Some(value))
                && resource
                    .as_ref()
                    .is_none_or(|value| parsed.as_ref().map(|parts| &parts.resource) == Some(value))
                && query.as_ref().is_none_or(|needle| {
                    subscription.pattern.contains(needle)
                        || subscription.session_id.contains(needle)
                        || subscription.subscription_id.to_string().contains(needle)
                })
            {
                let stats = route_stats.get(&(family, subscription.pattern.clone()));
                Some(NoticeDeliveryObservation {
                    route_family: family,
                    realm: subscription.realm,
                    area: parsed.as_ref().map(|parts| parts.area.clone()),
                    resource: parsed.as_ref().map(|parts| parts.resource.clone()),
                    route: subscription.pattern,
                    session_id: Some(subscription.session_id),
                    subscription_id: Some(subscription.subscription_id),
                    status: "active_subscription".to_string(),
                    notifications_received: subscription.notifications_received,
                    publishes_total: stats.map_or(0, |item| item.publishes_total),
                    publishes_per_minute: stats.map_or(0.0, |item| item.publishes_per_minute),
                })
            } else {
                None
            }
        })
        .take(limit)
        .collect();

    crate::api::admin::json_response(NoticeDeliveryObservationList {
        route_family: family,
        limit,
        observations,
    })
}

pub async fn notice_subscriptions_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> Result<Response, Infallible> {
    let subscriptions = runtime
        .notice_list_subscriptions(Some(path.realm), None)
        .into_iter()
        .filter(|subscription| {
            matches_family(family, subscription.route_family)
                && matches_resource_route(&subscription.pattern, path)
        })
        .collect();
    crate::api::admin::json_response(NoticeSubscriptionsList { subscriptions })
}

pub async fn notice_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> Result<Response, Infallible> {
    let subscriptions = runtime
        .notice_list_subscriptions(Some(path.realm), None)
        .into_iter()
        .filter(|subscription| matches_family(family, subscription.route_family))
        .collect::<Vec<_>>();
    let routes = runtime
        .notice_list_routes(Some(path.realm))
        .into_iter()
        .filter(|route| matches_family(family, route.route_family))
        .collect::<Vec<_>>();
    crate::api::admin::json_response(troubleshooting::notice_resource_timeline(
        &subscriptions,
        &routes,
        path,
        limit,
    ))
}
