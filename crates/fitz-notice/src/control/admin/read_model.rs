use super::model::{NoticeRouteInfo, NoticeSubscription};
use fitz_core::projection::{FamilyRow, FamilyRows};
use parking_lot::{RwLock, RwLockReadGuard};
use std::sync::Arc;

fn matches_realm(realm: Option<&str>, value: &str) -> bool {
    realm.is_none_or(|needle| value == needle)
}

fn matches_substring(filter: Option<&str>, value: &str) -> bool {
    filter.is_none_or(|needle| value.contains(needle))
}

fn matches_notice_route_realm(realm: Option<&str>, route: &str) -> bool {
    realm.is_none_or(|needle| route_triplet(route).is_some_and(|parts| parts.realm == needle))
}

fn collect_slice_matches<T: Clone + FamilyRow>(
    items: &FamilyRows<T>,
    include: impl Fn(&T) -> bool,
) -> Vec<T> {
    items.iter().filter(|item| include(item)).cloned().collect()
}
use fitz_core::runtime::routing::route_triplet;

#[derive(Default)]
pub struct AdminReadModel {
    notice_subscriptions: RwLock<FamilyRows<NoticeSubscription>>,
    notice_routes: RwLock<FamilyRows<NoticeRouteInfo>>,
}

impl AdminReadModel {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn replace_notice_subscriptions(&self, subscriptions: Vec<NoticeSubscription>) {
        *self.notice_subscriptions.write() = subscriptions.into();
    }

    pub fn replace_notice_family_subscriptions(
        &self,
        route_family: u64,
        subscriptions: Vec<NoticeSubscription>,
    ) {
        let mut current = self.notice_subscriptions.write();
        current.retain(|subscription| subscription.route_family != route_family);
        current.extend(subscriptions);
    }

    pub fn notice_subscriptions(
        &self,
        realm: Option<&str>,
        route_pattern: Option<&str>,
    ) -> Vec<NoticeSubscription> {
        let subscriptions = self.notice_subscriptions.read();
        collect_slice_matches(&subscriptions, |item| {
            matches_realm(realm, &item.realm) && matches_substring(route_pattern, &item.pattern)
        })
    }

    pub fn replace_notice_routes(&self, routes: Vec<NoticeRouteInfo>) {
        *self.notice_routes.write() = routes.into();
    }

    pub fn replace_notice_family_routes(&self, route_family: u64, routes: Vec<NoticeRouteInfo>) {
        let mut current = self.notice_routes.write();
        current.retain(|route| route.route_family != route_family);
        current.extend(routes);
    }

    pub fn notice_routes(&self, realm: Option<&str>) -> Vec<NoticeRouteInfo> {
        let routes = self.notice_routes.read();
        collect_slice_matches(&routes, |item| {
            matches_notice_route_realm(realm, &item.route)
        })
    }

    #[doc(hidden)]
    pub fn notice_subscriptions_rows(&self) -> RwLockReadGuard<'_, FamilyRows<NoticeSubscription>> {
        self.notice_subscriptions.read()
    }

    #[doc(hidden)]
    pub fn notice_routes_rows(&self) -> RwLockReadGuard<'_, FamilyRows<NoticeRouteInfo>> {
        self.notice_routes.read()
    }
}
