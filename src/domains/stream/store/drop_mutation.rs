//! Destructive namespace resets and their durable visibility fences.

use super::drop_scope::{cutoff_key, cutoff_snapshot};
use super::{
    encode_global_counter_key, encode_stream_layout_marker_key, RealmCounterValue, StreamStore,
    StreamWriteMode,
};
use crate::domains::stream::{StreamDropScope, StreamDropTarget};
use crate::utils::storage_key::{self, DomainKeyspace};
use std::time::{Duration, Instant};

const ADMISSION_BUDGET: Duration = Duration::from_secs(30);

pub(super) fn timeout() -> String {
    format!(
        "{}Stream DROP storage admission deadline expired",
        crate::domains::stream::protocol::ERR_STREAM_TIMEOUT
    )
}

fn delete_prefix(
    transaction: &mut cntryl_midge::Transaction,
    prefix: Vec<u8>,
) -> Result<(), String> {
    let end = storage_key::prefix_range_end(&prefix);
    transaction
        .delete_range(prefix, end)
        .map_err(|error| format!("delete Stream namespace failed: {error:?}"))
}

fn component_prefix(
    realm: &str,
    kind: super::KeyPrefix,
    area: &str,
    resource: Option<&str>,
) -> Vec<u8> {
    let mut encoder = storage_key::domain_marker_encoder(
        realm,
        DomainKeyspace::Stream,
        kind as u8,
        area.len() + resource.map_or(0, str::len) + 2,
    );
    storage_key::encode_segment_into(&mut encoder, area);
    if let Some(resource) = resource {
        storage_key::encode_segment_into(&mut encoder, resource);
    }
    encoder.into_vec()
}

fn delete_namespace(
    transaction: &mut cntryl_midge::Transaction,
    scope: &StreamDropScope,
) -> Result<(), String> {
    use super::KeyPrefix;
    let (area, resource) = match &scope.target {
        StreamDropTarget::Realm => {
            return delete_prefix(
                transaction,
                storage_key::domain_prefix(&scope.realm, DomainKeyspace::Stream),
            )
        }
        StreamDropTarget::Area { area } => (area.as_str(), None),
        StreamDropTarget::Resource { area, resource } => (area.as_str(), Some(resource.as_str())),
    };
    for kind in [
        KeyPrefix::Resource,
        KeyPrefix::CompactResourcePage,
        KeyPrefix::ResourceDiscriminator,
    ] {
        delete_prefix(
            transaction,
            component_prefix(&scope.realm, kind, area, resource),
        )?;
    }
    if let Some(resource) = resource {
        for key in [
            super::encode_resource_meta_key(&scope.realm, area, resource),
            crate::domains::stream::storage::encode_offset_counter_key(
                &scope.realm,
                area,
                resource,
            ),
        ] {
            transaction.delete(key).map_err(|error| error.to_string())?;
        }
    } else {
        for kind in [
            KeyPrefix::ResourceMeta,
            KeyPrefix::OffsetCounter,
            KeyPrefix::Area,
            KeyPrefix::CompactAreaPage,
            KeyPrefix::AreaDiscriminator,
        ] {
            delete_prefix(
                transaction,
                component_prefix(&scope.realm, kind, area, None),
            )?;
        }
        for key in [
            super::encode_area_counter_key(&scope.realm, area),
            super::encode_watermark_key(&scope.realm, area),
        ] {
            transaction.delete(key).map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

impl StreamStore {
    fn prepare_drop_transaction(
        &self,
        family: u32,
        scope: &StreamDropScope,
    ) -> Result<cntryl_midge::Transaction, String> {
        let (area, resource) = match &scope.target {
            StreamDropTarget::Realm => (None, None),
            StreamDropTarget::Area { area } => (Some(area.as_str()), None),
            StreamDropTarget::Resource { area, resource } => {
                (Some(area.as_str()), Some(resource.as_str()))
            }
        };
        let mut transaction = self
            .db
            .begin_tx(family, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|error| error.to_string())?;
        transaction.set_conflict_policy(cntryl_midge::ConflictPolicy::AbortOnWriteConflict);
        let counter_key = encode_global_counter_key();
        let counter_bytes = transaction
            .get(&counter_key)
            .map_err(|error| error.to_string())?;
        let head = counter_bytes.as_ref().map_or(Ok(0), |bytes| {
            RealmCounterValue::decode(bytes).map(|value| value.next_offset)
        })?;
        transaction
            .assert_value(counter_key, counter_bytes.map(|value| value.to_vec()))
            .map_err(|error| error.to_string())?;
        let cutoffs = cutoff_snapshot(&transaction, &scope.realm, area, resource)?;
        cutoffs.assert_unchanged(&mut transaction)?;
        delete_namespace(&mut transaction, scope)?;
        transaction
            .put(
                cutoff_key(&scope.realm, area, resource),
                head.max(cutoffs.maximum).to_le_bytes().to_vec(),
                None,
            )
            .map_err(|error| error.to_string())?;
        // Older D4 brokers must reject a family whose deletion fences they cannot honor.
        transaction
            .put(encode_stream_layout_marker_key(), vec![0, 0xD5, 2], None)
            .map_err(|error| error.to_string())?;
        if head > cutoffs.maximum {
            super::drop_reclamation::queue_reclamation(&mut transaction, head)?;
        }
        Ok(transaction)
    }

    /// Destroy a resource namespace, area, or realm under an explicit write mode.
    ///
    /// Callers coordinate live append sessions before invoking this storage operation.
    ///
    /// # Errors
    /// Returns an error for an invalid scope, expired admission budget, or failed commit.
    pub fn drop_scope(&self, scope: &StreamDropScope, mode: StreamWriteMode) -> Result<(), String> {
        self.drop_scope_with_deadline(scope, mode, None)
    }

    pub(crate) fn drop_scope_with_deadline(
        &self,
        scope: &StreamDropScope,
        mode: StreamWriteMode,
        request_deadline: Option<Instant>,
    ) -> Result<(), String> {
        scope.validate()?;
        let family = crate::storage::cf_validation::route_family_to_cf(scope.route_family)?;
        let deadline = request_deadline.map_or_else(
            || Instant::now() + ADMISSION_BUDGET,
            |deadline| deadline.min(Instant::now() + ADMISSION_BUDGET),
        );
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        let guard = {
            let mut guards = self
                .family_sequence_guards
                .try_lock_until(deadline)
                .ok_or_else(timeout)?;
            guards.entry(u64::from(family)).or_default().clone()
        };
        let _lock = guard.try_lock_until(deadline).ok_or_else(timeout)?;
        let recovered = self
            .recovered_global_families
            .try_lock_until(deadline)
            .ok_or_else(timeout)?
            .contains(&u64::from(family));
        if !recovered {
            if Instant::now() >= deadline {
                return Err(timeout());
            }
            self.inspect_and_activate_layout_for_family(u64::from(family))?;
            self.recover_global_ordering_locked(u64::from(family), Some(deadline))?;
        }
        let options = match mode {
            StreamWriteMode::Sync => self.sync_write_options,
            StreamWriteMode::Buffered => self.buffered_write_options,
        };
        loop {
            if Instant::now() >= deadline {
                return Err(timeout());
            }
            let transaction = self.prepare_drop_transaction(family, scope)?;
            if Instant::now() >= deadline {
                return Err(timeout());
            }
            match Self::commit_storage_transaction(transaction, options) {
                Ok(()) => {
                    self.resource_meta_states.lock().retain(
                        |(family, realm, area, resource), _| {
                            *family != scope.route_family.as_u64()
                                || !scope.matches_parts(scope.route_family, realm, area, resource)
                        },
                    );
                    #[cfg(test)]
                    self.realm_sequence_states
                        .lock()
                        .retain(|(family, realm), _| {
                            *family != scope.route_family.as_u64() || realm != &scope.realm
                        });
                    return Ok(());
                }
                Err(cntryl_midge::MidgeError::WriteConflict(_)) => {}
                Err(error)
                    if crate::storage::write_admission::is_l0_admission_rejection(
                        &error, family,
                    ) =>
                {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero()
                        || !self
                            .db
                            .wait_for_write_stall_clear(family, remaining)
                            .map_err(|error| error.to_string())?
                    {
                        return Err(timeout());
                    }
                    std::thread::yield_now();
                }
                Err(error) => return Err(format!("Stream DROP commit failed: {error:?}")),
            }
        }
    }
}
