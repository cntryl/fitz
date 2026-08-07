// KV domain sink for session-scoped transaction dispatch.
//
// Committed KV writes flow straight to Midge and persist according to the
// `WriteOptions` selected when the transaction commits. Active `tx_id`
// handles, resource locks, and admin snapshot entries are separate live
// in-memory state owned by the current broker process. `cleanup_session`
// intentionally discards that state on disconnect, and broker restart clears
// it wholesale instead of attempting transaction recovery.

#[cfg(test)]
pub(super) use crate::dispatch::protocol::frame_context::FrameContext;
pub(super) use crate::domains::kv::{KvClientFrame, KvClientRequest};
pub(super) use crate::runtime::routing::RouteFamily;
pub(super) use crate::runtime::{DeliveryError, Envelope, MailboxSink, ManagedActor, Router};
pub(super) use bytes::Bytes;
#[cfg(test)]
pub(super) use chrono::Utc;
pub(super) use parking_lot::Mutex;
pub(super) use std::collections::HashMap;
pub(super) use std::sync::atomic::{AtomicBool, Ordering};
pub(super) use std::sync::Arc;

pub type AdminKvCommittedPair = (Vec<u8>, Vec<u8>);
pub type AdminKvPrefixScanResult = (Vec<AdminKvCommittedPair>, bool);
pub type AdminKvRowsResult = (Vec<AdminKvCommittedPair>, Option<Vec<u8>>, bool);

pub struct AdminKvRowsRequest<'a> {
    pub route_family: RouteFamily,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub starts_with: &'a [u8],
    pub cursor: Option<&'a [u8]>,
    pub limit: usize,
}

pub(super) const ADMIN_INVENTORY_REFRESH_LIMIT: usize = 10_000;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct KvResourceLockKey {
    pub(super) family_id: u64,
    pub(super) realm: String,
    pub(super) area: String,
    pub(super) resource: String,
}

impl KvResourceLockKey {
    pub(super) fn new(family_id: u64, realm: &str, area: &str, resource: &str) -> Self {
        Self {
            family_id,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
        }
    }
}

pub(super) struct KvDomainCore {
    pub(super) store: Arc<cntryl_midge::Engine>,
    pub(super) actors: Arc<Mutex<HashMap<u64, crate::domains::kv::KvActor>>>,
    pub(super) watch_actors: Mutex<HashMap<u64, crate::domains::kv::watch::KvWatchActor>>,
    pub(super) router: Arc<Router>,
    pub(super) projection: crate::domains::kv::projection::KvAdminProjection<KvResourceLockKey>,
    pub(super) metrics: Option<crate::domains::kv::KvMetrics>,
    pub(super) sync_write_options: cntryl_midge::WriteOptions,
    pub(super) buffered_write_options: cntryl_midge::WriteOptions,
}

pub(super) struct KvDomainState {
    pub(super) core: KvDomainCore,
    pub(super) active: AtomicBool,
}

pub(super) struct KvDomainRuntime<'a> {
    pub(super) core: &'a KvDomainCore,
    pub(super) active: &'a AtomicBool,
}

pub(super) enum KvAdminTransactionUpdate {
    None,
    Upsert(crate::control::admin::KvTransaction),
    Remove { session_id: u64, tx_id: u64 },
}

pub(super) enum KvDomainCommand {
    Deliver(Envelope),
    CleanupSession(u64, crossbeam_channel::Sender<()>),
    ReadActiveTransactionCount(crossbeam_channel::Sender<usize>),
    #[cfg(test)]
    SyncAdminSnapshot(crossbeam_channel::Sender<()>),
    #[cfg(test)]
    ReadLatencySnapshots(
        KvResourceLockKey,
        crossbeam_channel::Sender<(
            crate::control::admin::KvLatencySnapshot,
            crate::control::admin::KvLatencySnapshot,
        )>,
    ),
    #[cfg(test)]
    ApplyWriteOptions(
        crate::domains::kv::KvMessage,
        crossbeam_channel::Sender<crate::domains::kv::KvMessage>,
    ),
    #[cfg(test)]
    PanicForTests,
}

pub(super) struct KvDomainActor {
    pub(super) state: Arc<KvDomainState>,
}

pub struct KvDomainSink {
    pub(super) state: Arc<KvDomainState>,
    pub(super) actor: ManagedActor<KvDomainCommand>,
}
