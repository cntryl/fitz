//! Resource selection happens in the shared indices before row copies or result limits.
use super::family_rows::ResourceIdentity;
use super::{AdminReadModel, AdminSnapshot, InventoryScope};
use crate::runtime::DomainKind;

impl AdminReadModel {
    pub(crate) fn bounded_resource_snapshot(
        &self,
        families: &[u64],
        scope: InventoryScope<'_>,
        domain: DomainKind,
        limit: usize,
    ) -> AdminSnapshot {
        let mut snapshot = AdminSnapshot {
            limit_per_collection: limit,
            ..AdminSnapshot::default()
        };
        for &family in families {
            let identity: ResourceIdentity = (
                family,
                scope.realm.expect("resource realm").into(),
                scope.area.expect("resource area").into(),
                scope.resource.expect("resource name").into(),
            );
            macro_rules! copy_rows {
                ($field:ident) => {{
                    snapshot.$field.extend(self.$field.read().resource_snapshot(
                        &identity,
                        limit.saturating_sub(snapshot.$field.len()),
                        &mut snapshot.truncated,
                    ));
                }};
            }
            match domain {
                DomainKind::Kv => copy_rows!(kv_transactions),
                DomainKind::Stream => copy_rows!(streams),
                DomainKind::Notice => {
                    copy_rows!(notice_subscriptions);
                    copy_rows!(notice_routes);
                }
                DomainKind::Queue => {
                    copy_rows!(queues);
                    copy_rows!(queue_inflight);
                    copy_rows!(queue_dead_letters);
                }
                DomainKind::Rpc => {
                    copy_rows!(rpc_workers);
                    copy_rows!(rpc_pending);
                }
                DomainKind::Lease => {
                    if let Some(lease) = self.leases.read().get(&identity) {
                        if snapshot.leases.len() < limit {
                            snapshot.leases.push(lease.clone());
                        } else {
                            snapshot.truncated = true;
                        }
                    }
                }
                DomainKind::Schedule => {
                    let schedules = self.schedules.read();
                    let start = (
                        identity.0,
                        identity.1.clone(),
                        identity.2.clone(),
                        identity.3.clone(),
                        String::new(),
                    );
                    let mut rows = schedules.range(start..).take_while(|(key, _)| {
                        key.0 == identity.0
                            && key.1 == identity.1
                            && key.2 == identity.2
                            && key.3 == identity.3
                    });
                    snapshot.schedules.extend(
                        rows.by_ref()
                            .take(limit.saturating_sub(snapshot.schedules.len()))
                            .map(|(_, row)| row.clone()),
                    );
                    snapshot.truncated |= rows.next().is_some();
                }
            }
        }
        snapshot
    }
}
