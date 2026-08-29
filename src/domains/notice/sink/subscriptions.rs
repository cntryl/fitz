//! Subscribe/unsubscribe message handling: mutation of the live subscription
//! index in response to a client request.

use super::{
    subscription_limit_error, Arc, NoticeDomainCore, NoticeSubscription, Ordering,
    RoutedSubscriptionSet,
};

impl NoticeDomainCore {
    pub(super) fn rollback_undeliverable_subscribe(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        session_id: u64,
        response: &crate::domains::notice::NoticeResponse,
    ) -> bool {
        let crate::domains::notice::NoticeResponse::SubscribeOk { subscription_id } = response
        else {
            return false;
        };
        let mut families = self.families.lock();
        let removed = families.get_mut(&family_id).is_some_and(|state| {
            state.remove_subscription_for_session(family_id, session_id, *subscription_id)
        });
        if families
            .get(&family_id)
            .is_some_and(RoutedSubscriptionSet::is_empty)
        {
            families.remove(&family_id);
        }
        drop(families);
        if removed {
            self.mark_admin_snapshot_dirty();
        }
        removed
    }

    pub(super) fn dispatch_notice_message(
        &self,
        notice_msg: crate::domains::notice::protocol::NotificationMessage,
    ) -> (Option<crate::domains::notice::NoticeResponse>, bool) {
        use crate::domains::notice::protocol::NotificationMessage;
        use crate::domains::notice::NoticeResponse;

        match notice_msg {
            NotificationMessage::Publish(pub_msg) => {
                self.publish_route_payload(pub_msg.family_id, &pub_msg.route, &pub_msg.payload);
                (None, false)
            }
            NotificationMessage::Subscribe(sub_msg) => self.handle_subscribe_message(&sub_msg),
            NotificationMessage::Unsubscribe(unsub_msg) => {
                let family_id = unsub_msg.family_id;
                let mut families = self.families.lock();
                let removed = if let Some(state) = families.get_mut(&family_id) {
                    let removed = state.remove_subscription_for_session(
                        unsub_msg.family_id,
                        unsub_msg.session_id.0,
                        unsub_msg.subscription_id,
                    );
                    if state.is_empty() {
                        families.remove(&family_id);
                    }
                    removed
                } else {
                    false
                };
                if removed {
                    self.counter_add("fitz_notice_unsubscribes_total", 1);
                }
                (Some(NoticeResponse::Ok), removed)
            }
            NotificationMessage::UnsubscribeAll(unsub_all) => {
                let session_id = unsub_all.session_id.0;
                let removed = self.unsubscribe_all_for_session(session_id);
                tracing::debug!(
                    domain = "notice",
                    session = session_id,
                    "All subscriptions removed for session"
                );
                (Some(NoticeResponse::Ok), removed > 0)
            }
            NotificationMessage::Deliver(_) => (Some(NoticeResponse::Ok), false),
        }
    }

    fn handle_subscribe_message(
        &self,
        sub_msg: &crate::domains::notice::protocol::SubscribeMessage,
    ) -> (Option<crate::domains::notice::NoticeResponse>, bool) {
        if let Some(response) = self.try_reuse_existing(sub_msg) {
            return (Some(response), false);
        }
        let compiled = match Self::compile_pattern(sub_msg) {
            Ok(compiled) => compiled,
            Err(response) => return (Some(response), false),
        };
        let (response, state_changed) = self.allocate_and_insert(sub_msg, compiled);
        (Some(response), state_changed)
    }

    fn compile_pattern(
        sub_msg: &crate::domains::notice::protocol::SubscribeMessage,
    ) -> Result<crate::runtime::matcher::Pattern, crate::domains::notice::NoticeResponse> {
        crate::runtime::DomainKind::Notice
            .descriptor()
            .compile_registration_pattern(sub_msg.pattern.as_str())
            .map_err(|error| {
                tracing::warn!(
                    domain = "notice",
                    session = sub_msg.session_id.0,
                    "Rejected invalid subscription pattern"
                );
                crate::domains::notice::NoticeResponse::Error(error)
            })
    }

    fn try_reuse_existing(
        &self,
        sub_msg: &crate::domains::notice::protocol::SubscribeMessage,
    ) -> Option<crate::domains::notice::NoticeResponse> {
        let families = self.families.lock();
        let id = families.get(&sub_msg.family_id).and_then(|state| {
            state.find_existing_id(sub_msg.session_id.0, sub_msg.pattern.as_str())
        })?;
        tracing::debug!(
            domain = "notice",
            session = sub_msg.session_id.0,
            subscription_id = id,
            pattern = sub_msg.pattern.as_str(),
            "Notice subscription already exists (idempotent)"
        );
        Some(crate::domains::notice::NoticeResponse::SubscribeOk {
            subscription_id: id,
        })
    }

    fn allocate_and_insert(
        &self,
        sub_msg: &crate::domains::notice::protocol::SubscribeMessage,
        compiled: crate::runtime::matcher::Pattern,
    ) -> (crate::domains::notice::NoticeResponse, bool) {
        use crate::domains::notice::NoticeResponse;

        let mut families = self.families.lock();
        let session_subscription_count = families
            .values()
            .map(|state| state.subscription_count_for_session(sub_msg.session_id.0))
            .sum::<usize>();
        let state = families
            .entry(sub_msg.family_id)
            .or_insert_with(RoutedSubscriptionSet::new);

        let (response, state_changed) = if let Some(error) =
            subscription_limit_error(state, session_subscription_count, sub_msg, &compiled)
        {
            (error, false)
        } else {
            let Ok(new_id) =
                self.next_sub_id
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        current.checked_add(1)
                    })
            else {
                let state_empty = state.is_empty();
                if state_empty {
                    families.remove(&sub_msg.family_id);
                }
                return (
                    NoticeResponse::Error("subscription ID space exhausted".to_string()),
                    false,
                );
            };
            state.insert(
                sub_msg.family_id,
                NoticeSubscription {
                    pattern: compiled,
                    pattern_route: Arc::from(sub_msg.pattern.as_str()),
                    session_id: sub_msg.session_id.0,
                    subscription_id: new_id,
                    subscriber: sub_msg.subscriber.clone(),
                },
            );

            tracing::debug!(
                domain = "notice",
                session = sub_msg.session_id.0,
                subscription_id = new_id,
                pattern = sub_msg.pattern.as_str(),
                "Notice subscription added"
            );
            (
                NoticeResponse::SubscribeOk {
                    subscription_id: new_id,
                },
                true,
            )
        };

        (response, state_changed)
    }
}
