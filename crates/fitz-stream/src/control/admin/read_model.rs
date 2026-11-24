use super::model::{StreamAreaWatermarkDetail, StreamInfo, StreamRealmWatermarkDetail};
use fitz_core::projection::{FamilyRow, FamilyRows};
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::BTreeMap;
use std::sync::Arc;
type StreamRealmIdentity = String;
type StreamAreaIdentity = (String, String);

fn matches_realm(realm: Option<&str>, value: &str) -> bool {
    realm.is_none_or(|needle| value == needle)
}

fn collect_slice_matches<T: Clone + FamilyRow>(
    items: &FamilyRows<T>,
    include: impl Fn(&T) -> bool,
) -> Vec<T> {
    items.iter().filter(|item| include(item)).cloned().collect()
}

#[derive(Default)]
pub struct AdminReadModel {
    streams: RwLock<FamilyRows<StreamInfo>>,
    stream_realm_watermarks: RwLock<BTreeMap<StreamRealmIdentity, StreamRealmWatermarkDetail>>,
    stream_area_watermarks: RwLock<BTreeMap<StreamAreaIdentity, StreamAreaWatermarkDetail>>,
    stream_events_total: RwLock<usize>,
}

impl AdminReadModel {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn replace_streams(&self, streams: Vec<StreamInfo>) {
        *self.streams.write() = streams.into();
    }

    pub fn streams(&self, realm: Option<&str>) -> Vec<StreamInfo> {
        let streams = self.streams.read();
        collect_slice_matches(&streams, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_stream_realm_watermarks(&self, watermarks: Vec<StreamRealmWatermarkDetail>) {
        *self.stream_realm_watermarks.write() = watermarks
            .into_iter()
            .map(|detail| (detail.realm.clone(), detail))
            .collect();
    }

    pub fn stream_realm_watermark(&self, realm: &str) -> Option<StreamRealmWatermarkDetail> {
        self.stream_realm_watermarks.read().get(realm).cloned()
    }

    pub fn stream_realm_watermarks(&self) -> Vec<StreamRealmWatermarkDetail> {
        self.stream_realm_watermarks
            .read()
            .values()
            .cloned()
            .collect()
    }

    pub fn replace_stream_area_watermarks(&self, watermarks: Vec<StreamAreaWatermarkDetail>) {
        *self.stream_area_watermarks.write() = watermarks
            .into_iter()
            .map(|detail| ((detail.realm.clone(), detail.area.clone()), detail))
            .collect();
    }

    pub fn stream_area_watermark(
        &self,
        realm: &str,
        area: &str,
    ) -> Option<StreamAreaWatermarkDetail> {
        self.stream_area_watermarks
            .read()
            .get(&(realm.to_string(), area.to_string()))
            .cloned()
    }

    pub fn stream_area_watermarks(&self) -> Vec<StreamAreaWatermarkDetail> {
        self.stream_area_watermarks
            .read()
            .values()
            .cloned()
            .collect()
    }

    pub fn replace_stream_events_total(&self, total: usize) {
        *self.stream_events_total.write() = total;
    }

    pub fn stream_events_total(&self) -> usize {
        *self.stream_events_total.read()
    }

    #[doc(hidden)]
    pub fn streams_rows(&self) -> RwLockReadGuard<'_, FamilyRows<StreamInfo>> {
        self.streams.read()
    }

    #[doc(hidden)]
    pub fn stream_realm_watermarks_rows(
        &self,
    ) -> RwLockReadGuard<'_, BTreeMap<StreamRealmIdentity, StreamRealmWatermarkDetail>> {
        self.stream_realm_watermarks.read()
    }

    #[doc(hidden)]
    pub fn stream_area_watermarks_rows(
        &self,
    ) -> RwLockReadGuard<'_, BTreeMap<StreamAreaIdentity, StreamAreaWatermarkDetail>> {
        self.stream_area_watermarks.read()
    }
}
