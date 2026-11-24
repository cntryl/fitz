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
                ($domain:ident, $field:ident, $rows:ident) => {{
                    snapshot
                        .$field
                        .extend(self.$domain.$rows().resource_snapshot(
                            &identity,
                            limit.saturating_sub(snapshot.$field.len()),
                            &mut snapshot.truncated,
                        ));
                }};
            }
            match domain {
                DomainKind::Kv => copy_rows!(kv, kv_transactions, kv_transactions_rows),
                DomainKind::Stream => copy_rows!(stream, streams, streams_rows),
                DomainKind::Notice => {
                    copy_rows!(notice, notice_subscriptions, notice_subscriptions_rows);
                    copy_rows!(notice, notice_routes, notice_routes_rows);
                }
                DomainKind::Queue => {
                    copy_rows!(queue, queues, queues_rows);
                    copy_rows!(queue, queue_inflight, queue_inflight_rows);
                    copy_rows!(queue, queue_dead_letters, queue_dead_letters_rows);
                }
                DomainKind::Rpc => {
                    copy_rows!(rpc, rpc_workers, rpc_workers_rows);
                    copy_rows!(rpc, rpc_pending, rpc_pending_rows);
                }
                DomainKind::Lease => {
                    if let Some(lease) = self.lease.leases_rows().get(&identity) {
                        if snapshot.leases.len() < limit {
                            snapshot.leases.push(lease.clone());
                        } else {
                            snapshot.truncated = true;
                        }
                    }
                }
                DomainKind::Schedule => {
                    let schedules = self.schedule.schedules_rows();
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
