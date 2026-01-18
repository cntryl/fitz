use bytes::Bytes;
use std::sync::Arc;

use cntryl_midge::WriteOptions;

pub struct ScheduleStore {
    db: Arc<cntryl_midge::Engine>,
}

impl ScheduleStore {
    pub fn new(db: Arc<cntryl_midge::Engine>) -> Self {
        Self { db }
    }

    fn encode_key(family: u64, id: u64) -> Vec<u8> {
        // family and id into ASCII: "family:{family}:sch:{id:016x}"
        format!("family:{}:sch:{:016x}", family, id).into_bytes()
    }

    /// Persist schedule route + payload + last_fire_at (i64, seconds since epoch) as value.
    /// Value format:
    /// [8 bytes LE last_fire_at][4 bytes BE route_len][route bytes][payload bytes]
    pub fn insert(
        &self,
        family: u64,
        id: u64,
        route: &[u8],
        payload: Bytes,
        last_fire_at: i64,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        // Use RouteFamily id as Midge column family id to ensure isolation
        let mut txn = self
            .db
            .begin_tx(
                cntryl_midge::ColumnFamilyId(family as u32),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        let mut val = Vec::with_capacity(8 + 4 + route.len() + payload.len());
        val.extend(&last_fire_at.to_le_bytes());
        let route_len = (route.len() as u32).to_be_bytes();
        val.extend(&route_len);
        val.extend(route);
        val.extend(payload);

        txn.put(Self::encode_key(family, id), val, None)
            .map_err(|e| format!("put failed: {:?}", e))?;
        self.db
            .commit(txn, write_options)
            .map_err(|e| format!("commit failed: {:?}", e))?;
        Ok(())
    }

    pub fn delete(&self, family: u64, id: u64, write_options: WriteOptions) -> Result<(), String> {
        let mut txn = self
            .db
            .begin_tx(
                cntryl_midge::ColumnFamilyId(family as u32),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;
        txn.delete(Self::encode_key(family, id))
            .map_err(|e| format!("delete failed: {:?}", e))?;
        self.db
            .commit(txn, write_options)
            .map_err(|e| format!("commit failed: {:?}", e))?;
        Ok(())
    }

    pub fn list(&self, family: u64) -> Result<Vec<(u64, Bytes, Bytes, i64)>, String> {
        let txn = self
            .db
            .begin_tx(
                cntryl_midge::ColumnFamilyId(family as u32),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        let prefix = format!("family:{}:sch:", family);
        let mut out = Vec::new();
        // Use a prefix scan via Query
        let query = cntryl_midge::Query::new().prefix(Bytes::from(prefix.into_bytes()));
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan failed: {:?}", e))?;
        let results = iter.collect_all();

        for (k, v) in results {
            // parse id from key
            let keystr = String::from_utf8_lossy(&k);
            if let Some(pos) = keystr.rfind(":sch:") {
                let id_hex = &keystr[pos + 5..];
                if let Ok(id) = u64::from_str_radix(id_hex, 16) {
                    if v.len() >= 8 + 4 {
                        let mut arr = [0u8; 8];
                        arr.copy_from_slice(&v[..8]);
                        let last = i64::from_le_bytes(arr);
                        let route_len = u32::from_be_bytes([v[8], v[9], v[10], v[11]]) as usize;
                        if v.len() >= 8 + 4 + route_len {
                            let route_bytes = Bytes::copy_from_slice(&v[12..12 + route_len]);
                            let payload = Bytes::copy_from_slice(&v[12 + route_len..]);
                            out.push((id, route_bytes, payload, last));
                        }
                    }
                }
            }
        }

        Ok(out)
    }
}
