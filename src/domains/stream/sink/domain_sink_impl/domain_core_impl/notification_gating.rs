use super::{
    route_triplet, PendingStreamNotification, ReadyStreamNotification, StreamDomainCore,
    StreamNotificationTarget, StreamVisibilityFrontier,
};

const MAX_PENDING_NOTIFICATIONS: usize = 10_000;

#[derive(Default)]
struct VisibilityCache {
    areas: std::collections::HashMap<(String, String), Option<u64>>,
    realms: std::collections::HashMap<String, Option<u64>>,
    global: Option<u64>,
    global_loaded: bool,
}

#[derive(Clone, Copy)]
struct InvalidVisibilityEvent;

impl StreamDomainCore {
    fn visibility_frontier(
        selector: &str,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<StreamVisibilityFrontier, InvalidVisibilityEvent> {
        use crate::domains::stream::route_grammar::StreamRouteShape;

        let payload: serde_json::Value =
            serde_json::from_slice(&event.payload).map_err(|_| InvalidVisibilityEvent)?;
        if payload.get("event").and_then(serde_json::Value::as_str) != Some("committed") {
            return Err(InvalidVisibilityEvent);
        }
        let parts = route_triplet(event.route.as_str()).ok_or(InvalidVisibilityEvent)?;
        let shape = crate::domains::stream::route_grammar::classify_stream_route_shape(selector)
            .map_err(|_| InvalidVisibilityEvent)?;
        match shape {
            StreamRouteShape::Resource { .. } => Ok(StreamVisibilityFrontier::Resource),
            StreamRouteShape::Area { .. } => Ok(StreamVisibilityFrontier::Area {
                realm: parts.realm.to_string(),
                area: parts.area.to_string(),
                last_offset: payload
                    .get("last_area_offset")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(InvalidVisibilityEvent)?,
            }),
            StreamRouteShape::RealmFilterResource { .. } | StreamRouteShape::Realm { .. } => {
                Ok(StreamVisibilityFrontier::Realm {
                    realm: parts.realm.to_string(),
                    last_offset: payload
                        .get("last_realm_offset")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or(InvalidVisibilityEvent)?,
                })
            }
            StreamRouteShape::GlobalFilterAreaResource { .. }
            | StreamRouteShape::GlobalFilterArea { .. }
            | StreamRouteShape::GlobalFilterResource { .. }
            | StreamRouteShape::Global => Ok(StreamVisibilityFrontier::Global {
                last_offset: payload
                    .get("last_global_offset")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(InvalidVisibilityEvent)?,
            }),
        }
    }

    fn frontier_is_visible(
        &self,
        family: u64,
        frontier: &StreamVisibilityFrontier,
        cache: &mut VisibilityCache,
    ) -> bool {
        match frontier {
            StreamVisibilityFrontier::Resource => true,
            StreamVisibilityFrontier::Area {
                realm,
                area,
                last_offset,
            } => cache
                .areas
                .entry((realm.clone(), area.clone()))
                .or_insert_with(|| self.stream_store.get_watermark(family, realm, area).ok())
                .is_some_and(|watermark| watermark >= *last_offset),
            StreamVisibilityFrontier::Realm { realm, last_offset } => cache
                .realms
                .entry(realm.clone())
                .or_insert_with(|| self.stream_store.get_realm_watermark(family, realm).ok())
                .is_some_and(|watermark| watermark >= *last_offset),
            StreamVisibilityFrontier::Global { last_offset } => {
                if !cache.global_loaded {
                    cache.global = self.stream_store.get_global_watermark(family).ok();
                    cache.global_loaded = true;
                }
                cache
                    .global
                    .is_some_and(|watermark| watermark > *last_offset)
            }
        }
    }

    fn drain_visible_pending(
        &self,
        family: u64,
        cache: &mut VisibilityCache,
    ) -> Vec<ReadyStreamNotification> {
        let drained = {
            let mut pending = self.pending_notifications.lock();
            std::mem::take(&mut *pending)
        };
        let mut ready = Vec::new();
        let mut retained = Vec::with_capacity(drained.len());
        for notification in drained {
            if notification.event.family_id.as_u64() != family {
                retained.push(notification);
                continue;
            }
            if self.frontier_is_visible(family, &notification.frontier, cache) {
                ready.push(ReadyStreamNotification {
                    target: notification.target,
                    event: notification.event,
                });
            } else {
                retained.push(notification);
            }
        }
        let mut pending = self.pending_notifications.lock();
        let available = MAX_PENDING_NOTIFICATIONS.saturating_sub(retained.len());
        let dropped = pending.len().saturating_sub(available);
        let accepted = available.min(pending.len());
        retained.extend(pending.drain(..accepted));
        *pending = retained;
        if dropped > 0 {
            crate::observability::counter_add(
                crate::domains::stream::metrics::METRIC_NOTIFY_DROPS_TOTAL,
                u64::try_from(dropped).unwrap_or(u64::MAX),
            );
        }
        ready
    }

    pub(super) fn collect_ready_notifications(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Vec<ReadyStreamNotification> {
        let family = event.family_id.as_u64();
        let mut visibility_cache = VisibilityCache::default();
        let mut ready = self.drain_visible_pending(family, &mut visibility_cache);
        let matches = {
            let families = self.families.lock();
            let Some(state) = families.get(&family) else {
                return ready;
            };
            let mut matches = Vec::new();
            state.for_each_matching(event, |subscription| {
                matches.push((
                    StreamNotificationTarget {
                        session_id: subscription.session_id,
                        subscription_id: subscription.subscription_id,
                        subscriber: subscription.subscriber.clone(),
                    },
                    subscription.pattern.route().to_string(),
                ));
            });
            matches
        };
        let mut newly_pending = Vec::new();
        for (target, selector) in matches {
            match Self::visibility_frontier(&selector, event) {
                Ok(frontier)
                    if self.frontier_is_visible(family, &frontier, &mut visibility_cache) =>
                {
                    ready.push(ReadyStreamNotification {
                        target,
                        event: event.clone(),
                    });
                }
                Ok(frontier) => {
                    newly_pending.push(PendingStreamNotification {
                        target,
                        pattern: selector,
                        event: event.clone(),
                        frontier,
                    });
                }
                Err(_) => {
                    crate::observability::counter_inc(
                        crate::domains::stream::metrics::METRIC_NOTIFY_DROPS_TOTAL,
                    );
                }
            }
        }
        if !newly_pending.is_empty() {
            let mut pending = self.pending_notifications.lock();
            let available = MAX_PENDING_NOTIFICATIONS.saturating_sub(pending.len());
            let accepted = available.min(newly_pending.len());
            pending.extend(newly_pending.drain(..accepted));
            if !newly_pending.is_empty() {
                crate::observability::counter_add(
                    crate::domains::stream::metrics::METRIC_NOTIFY_DROPS_TOTAL,
                    u64::try_from(newly_pending.len()).unwrap_or(u64::MAX),
                );
            }
        }
        ready
    }

    pub(super) fn collect_visible_pending_notifications(
        &self,
        family: u64,
    ) -> Vec<ReadyStreamNotification> {
        self.drain_visible_pending(family, &mut VisibilityCache::default())
    }

    pub(super) fn remove_pending_notifications_for_session(&self, session_id: u64) {
        self.pending_notifications
            .lock()
            .retain(|pending| pending.target.session_id != session_id);
    }

    pub(in crate::domains::stream::sink) fn remove_pending_notifications_for_pattern(
        &self,
        session_id: u64,
        pattern: &str,
    ) {
        self.pending_notifications.lock().retain(|pending| {
            pending.target.session_id != session_id || pending.pattern != pattern
        });
    }
}
