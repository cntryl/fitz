use super::model::NoticeSubscription;
use crate::domains::notice::NoticeResponse;
use crate::domains::subscription_state::{
    RoutedSubscriptionSet, MAX_WILDCARD_REGISTRATIONS_PER_SESSION,
};

/// Total Notice registration cap for one ephemeral session.
pub(crate) const MAX_NOTICE_REGISTRATIONS_PER_SESSION: usize =
    crate::domains::subscription_state::MAX_TOTAL_REGISTRATIONS_PER_SESSION;

pub(super) fn subscription_limit_error(
    state: &RoutedSubscriptionSet<NoticeSubscription>,
    session_subscription_count: usize,
    sub_msg: &crate::domains::notice::protocol::SubscribeMessage,
    compiled: &crate::runtime::matcher::Pattern,
) -> Option<NoticeResponse> {
    match crate::domains::subscription_state::registration_limit_for_counts(
        session_subscription_count,
        compiled,
        state.wildcard_subscription_count_for_session(sub_msg.session_id.0),
    ) {
        Some(crate::domains::subscription_state::RegistrationLimit::Total) => {
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
        Some(crate::domains::subscription_state::RegistrationLimit::Wildcard) => {
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
        None => {}
    }

    None
}
