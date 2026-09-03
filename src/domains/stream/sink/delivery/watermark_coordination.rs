use super::super::model::{Envelope, Route, RouteFamily, StreamDomainCore};
use crate::domains::stream::metrics::METRIC_WATERMARK_COORDINATION_DROPS_TOTAL;
use crate::domains::stream::sink::model::{StreamAreaScope, StreamRealmScope};

struct WatermarkDispatch<'a, K> {
    address: crate::runtime::routing::RouteAddress,
    spawned: bool,
    family_id: RouteFamily,
    realm: &'a str,
    area: Option<&'a str>,
    coordinator: &'static str,
    commit: &'a crate::domains::stream::protocol::BatchCommitted,
    key_type: std::marker::PhantomData<K>,
}

impl StreamDomainCore {
    /// Notify the bounded area and realm coordinator pools after a durable
    /// batch commit. Visibility comes from the atomically committed counters;
    /// these actors persist advisory watermarks and emit ephemeral notices.
    pub(in crate::domains::stream::sink) fn notify_area_batch_committed(
        &self,
        family_id: RouteFamily,
        realm: &str,
        area: &str,
        commit: &crate::domains::stream::protocol::BatchCommitted,
    ) {
        let realm_address = crate::runtime::routing::RouteAddress::new(
            family_id,
            Route::new(format!(
                "stream://{realm}/{}",
                crate::domains::stream::INTERNAL_REALM_SEGMENT
            )),
        );
        let realm_spawned = {
            let store = self.stream_store.clone();
            let durable_metrics = self.durable_metrics.clone();
            let realm_owned = realm.to_string();
            self.watermark_coordinators.realm.ensure_spawned(
                StreamRealmScope {
                    family: family_id,
                    realm: realm.to_string(),
                },
                realm_address.clone(),
                move || {
                    crate::domains::stream::realm_actor::RealmActor::new(
                        family_id,
                        realm_owned.clone(),
                        store.clone(),
                        durable_metrics.clone(),
                    )
                },
            )
        };

        let area_address = crate::runtime::routing::RouteAddress::new(
            family_id,
            Route::new(format!(
                "stream://{realm}/{area}/{}",
                crate::domains::stream::INTERNAL_AREA_SEGMENT
            )),
        );
        let area_spawned = {
            let store = self.stream_store.clone();
            let durable_metrics = self.durable_metrics.clone();
            let realm_owned = realm.to_string();
            let area_owned = area.to_string();
            self.watermark_coordinators.area.ensure_spawned(
                StreamAreaScope {
                    family: family_id,
                    realm: realm.to_string(),
                    area: area.to_string(),
                },
                area_address.clone(),
                move || {
                    crate::domains::stream::area_actor::AreaActor::new(
                        family_id,
                        realm_owned.clone(),
                        area_owned.clone(),
                        store.clone(),
                        durable_metrics.clone(),
                    )
                },
            )
        };

        self.dispatch_watermark_commit(WatermarkDispatch::<(u64, String, String)> {
            address: area_address,
            spawned: area_spawned,
            family_id,
            realm,
            area: Some(area),
            coordinator: "area",
            commit,
            key_type: std::marker::PhantomData,
        });
        self.dispatch_watermark_commit(WatermarkDispatch::<(u64, String)> {
            address: realm_address,
            spawned: realm_spawned,
            family_id,
            realm,
            area: None,
            coordinator: "realm",
            commit,
            key_type: std::marker::PhantomData,
        });
    }

    fn dispatch_watermark_commit<K>(&self, dispatch: WatermarkDispatch<'_, K>) {
        let WatermarkDispatch {
            address,
            spawned,
            family_id,
            realm,
            area,
            coordinator,
            commit,
            key_type: _,
        } = dispatch;
        if !spawned {
            self.counter_inc(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL);
            tracing::warn!(
                domain = "stream",
                route_family = family_id.id(),
                realm,
                area = area.unwrap_or(""),
                coordinator,
                "Stream watermark coordinator capacity was exhausted"
            );
            return;
        }

        let envelope = Envelope::new(
            address,
            crate::domains::stream::protocol::StreamCoordinationMessage::BatchCommitted(
                commit.clone(),
            ),
        );
        if let Err(error) = self.router.route_high_priority(envelope) {
            self.counter_inc(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL);
            tracing::warn!(
                domain = "stream",
                route_family = family_id.id(),
                realm,
                area = area.unwrap_or(""),
                coordinator,
                error = ?error,
                "Stream watermark coordination notice was not accepted"
            );
        }
    }
}
