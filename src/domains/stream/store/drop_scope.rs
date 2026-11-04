use super::StreamStore;
use crate::utils::storage_key::{self, DomainKeyspace};
use bytes::Bytes;

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_DROP_CUTOFF_READ: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) struct CutoffSnapshot {
    values: Vec<(Vec<u8>, Option<Bytes>)>,
    pub(super) maximum: u64,
}

impl CutoffSnapshot {
    pub(super) fn assert_unchanged(
        &self,
        transaction: &mut cntryl_midge::Transaction,
    ) -> Result<(), String> {
        for (key, value) in &self.values {
            transaction
                .assert_value(key.clone(), value.as_ref().map(|bytes| bytes.to_vec()))
                .map_err(|error| format!("assert Stream deletion cutoff failed: {error:?}"))?;
        }
        Ok(())
    }
}

pub(super) fn cutoff_key(realm: &str, area: Option<&str>, resource: Option<&str>) -> Vec<u8> {
    let mut encoder = storage_key::domain_marker_encoder(
        "",
        DomainKeyspace::Stream,
        super::KeyPrefix::DropCutoff as u8,
        realm.len() + 16,
    );
    encoder.push_byte(if resource.is_some() {
        2
    } else {
        u8::from(area.is_some())
    });
    storage_key::encode_segment_into(&mut encoder, realm);
    if let Some(area) = area {
        storage_key::encode_segment_into(&mut encoder, area);
    }
    if let Some(resource) = resource {
        storage_key::encode_segment_into(&mut encoder, resource);
    }
    encoder.into_vec()
}

pub(super) fn decode_cutoff(value: &[u8]) -> Result<u64, String> {
    let bytes: [u8; 8] = value
        .try_into()
        .map_err(|_| "invalid Stream deletion cutoff".to_string())?;
    Ok(u64::from_le_bytes(bytes))
}

pub(super) fn cutoff_snapshot(
    transaction: &cntryl_midge::Transaction,
    realm: &str,
    area: Option<&str>,
    resource: Option<&str>,
) -> Result<CutoffSnapshot, String> {
    let mut keys = vec![cutoff_key(realm, None, None)];
    if let Some(area) = area {
        keys.push(cutoff_key(realm, Some(area), None));
        if let Some(resource) = resource {
            keys.push(cutoff_key(realm, Some(area), Some(resource)));
        }
    }
    let mut values = Vec::with_capacity(keys.len());
    let mut maximum = 0;
    for key in keys {
        let value = transaction
            .get(&key)
            .map_err(|error| format!("read Stream deletion cutoff failed: {error:?}"))?;
        if let Some(bytes) = &value {
            maximum = maximum.max(decode_cutoff(bytes)?);
        }
        values.push((key, value));
    }
    Ok(CutoffSnapshot { values, maximum })
}

impl StreamStore {
    pub(super) fn guard_append_reset(
        &self,
        txn: &mut cntryl_midge::Transaction,
        params: &super::CommitPromotionFrontierBatchParams<'_>,
    ) -> Result<(), super::PromotionWriteFailure> {
        let deletion =
            cutoff_snapshot(txn, params.realm, Some(params.area), Some(params.resource))?;
        if deletion.maximum != params.expected_drop_cutoff
            || params.first_global_offset < deletion.maximum
        {
            return Err(super::PromotionWriteFailure::Dropped);
        }
        deletion.assert_unchanged(txn)?;
        let resource_key =
            super::encode_resource_meta_key(params.realm, params.area, params.resource);
        let resource_bytes = txn
            .get(&resource_key)
            .map_err(|error| format!("read resource head failed: {error:?}"))?;
        let resource_head = resource_bytes.as_ref().map_or_else(
            || {
                self.scan_next_resource_offset(
                    params.family,
                    params.realm,
                    params.area,
                    params.resource,
                )
            },
            |bytes| super::ResourceMetaValue::decode(bytes).map(|value| value.next_offset),
        )?;
        if resource_head != params.first_resource_offset {
            return Err(super::PromotionWriteFailure::Other(
                super::StreamStoreError::ConcurrencyConflict
                    .store_code()
                    .to_string(),
            ));
        }
        txn.assert_value(resource_key, resource_bytes.map(|bytes| bytes.to_vec()))
            .map_err(|error| format!("assert resource head failed: {error:?}"))?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fail_next_drop_cutoff_read_for_tests() {
        FAIL_NEXT_DROP_CUTOFF_READ.with(|pending| pending.set(true));
    }
    pub(crate) fn resource_drop_cutoff(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<u64, String> {
        self.load_drop_cutoff(family, realm, Some(area), Some(resource))
    }

    pub(crate) fn area_drop_cutoff(
        &self,
        family: u64,
        realm: &str,
        area: &str,
    ) -> Result<u64, String> {
        self.load_drop_cutoff(family, realm, Some(area), None)
    }

    pub(crate) fn realm_drop_cutoff(&self, family: u64, realm: &str) -> Result<u64, String> {
        self.load_drop_cutoff(family, realm, None, None)
    }

    fn load_drop_cutoff(
        &self,
        family: u64,
        realm: &str,
        area: Option<&str>,
        resource: Option<&str>,
    ) -> Result<u64, String> {
        #[cfg(test)]
        if FAIL_NEXT_DROP_CUTOFF_READ.with(|pending| pending.replace(false)) {
            return Err("injected Stream deletion cutoff read failure".to_string());
        }
        let family = u32::try_from(family).map_err(|_| "invalid route family".to_string())?;
        let transaction = self
            .db
            .begin_tx(family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        cutoff_snapshot(&transaction, realm, area, resource).map(|snapshot| snapshot.maximum)
    }
}

pub(super) fn record_is_dropped(
    txn: &cntryl_midge::Transaction,
    realm: &str,
    area: &str,
    resource: &str,
    global_offset: u64,
) -> Result<bool, String> {
    Ok(global_offset < cutoff_snapshot(txn, realm, Some(area), Some(resource))?.maximum)
}
