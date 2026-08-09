use super::model::NoticeSubscription;
use crate::domains::notice::NoticeResponse;
use crate::domains::subscription_state::{
    RoutedSubscriptionSet, MAX_NOTICE_REGISTRATIONS_PER_SESSION,
    MAX_WILDCARD_REGISTRATIONS_PER_SESSION,
};

pub(super) fn subscription_limit_error(
    state: &RoutedSubscriptionSet<NoticeSubscription>,
    session_subscription_count: usize,
    sub_msg: &crate::domains::notice::protocol::SubscribeMessage,
    compiled: &crate::runtime::matcher::Pattern,
) -> Option<NoticeResponse> {
    if session_subscription_count >= MAX_NOTICE_REGISTRATIONS_PER_SESSION {
        tracing::warn!(
            domain = "notice",
            session = sub_msg.session_id.0,
            pattern = sub_msg.pattern.as_str(),
            limit = MAX_NOTICE_REGISTRATIONS_PER_SESSION,
            "Rejected notice subscription because session limit was exceeded"
        );
        crate::observability::counter_inc("fitz_notice_subscription_limit_rejects_total");
        return Some(NoticeResponse::Error(format!(
            "notice subscription limit exceeded ({MAX_NOTICE_REGISTRATIONS_PER_SESSION} per session)"
        )));
    }

    if state.wildcard_registration_limit_reached(sub_msg.session_id.0, compiled) {
        tracing::warn!(
            domain = "notice",
            session = sub_msg.session_id.0,
            pattern = sub_msg.pattern.as_str(),
            limit = MAX_WILDCARD_REGISTRATIONS_PER_SESSION,
            "Rejected wildcard notice subscription because session limit was exceeded"
        );
        crate::observability::counter_inc("fitz_notice_wildcard_limit_rejects_total");
        return Some(NoticeResponse::Error(format!(
            "wildcard subscription limit exceeded ({MAX_WILDCARD_REGISTRATIONS_PER_SESSION} per session)"
        )));
    }

    None
}
