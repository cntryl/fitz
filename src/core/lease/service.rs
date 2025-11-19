use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::types::*; // LeaseEntry, LeaseLock, etc.
use crate::routing::{GlobalInternTable, InternId, RouteFamilyId};
use dashmap::DashMap;
use parking_lot::RwLock;

/// Composite key for flat lease map:
/// (route_family, realm_id, area_id, resource_id)
/// All strings are interned once, eliminating repeated parsing and hashing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct LeaseKey {
    rf: RouteFamilyId,
    realm_id: InternId,
    area_id: InternId,
    resource_id: InternId,
}

/// Single flat map with composite keys - no nested structures.
type LeaseMap = DashMap<LeaseKey, LeaseLock>;

#[derive(Debug)]
pub struct LeaseService {
    /// Sharded flat maps for high concurrency
    shards: Vec<Arc<LeaseMap>>,
    /// String interner for lease key components
    interner: Arc<GlobalInternTable>,
    /// Fast inline UUID: rolling counter + random prefix
    id_counter: AtomicU64,
    id_prefix: u64,
    /// SipHash keys for fast token generation (replaces HMAC)
    token_key0: u64,
    token_key1: u64,
}

// === Public API =============================================================

impl LeaseService {
    pub fn new(interner: Arc<GlobalInternTable>) -> Arc<Self> {
        let shard_count = std::cmp::max(4, num_cpus::get());

        // Generate random prefix and SipHash keys once at startup
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};

        let random_state = RandomState::new();
        let mut hasher = random_state.build_hasher();
        hasher.write_u64(now_unix_secs());
        let id_prefix = hasher.finish();

        let mut hasher2 = random_state.build_hasher();
        hasher2.write_u64(id_prefix.wrapping_add(1));
        let token_key0 = hasher2.finish();

        let mut hasher3 = random_state.build_hasher();
        hasher3.write_u64(id_prefix.wrapping_add(2));
        let token_key1 = hasher3.finish();

        Arc::new(Self {
            shards: (0..shard_count)
                .map(|_| Arc::new(LeaseMap::new()))
                .collect(),
            interner,
            id_counter: AtomicU64::new(0),
            id_prefix,
            token_key0,
            token_key1,
        })
    }

    #[cfg(test)]
    fn new_for_test() -> Arc<Self> {
        Self::new(Arc::new(GlobalInternTable::new()))
    }

    #[inline]
    /// Fast inline ID generation: prefix + counter (no UUID library overhead)
    fn new_id(&self) -> u64 {
        let counter = self.id_counter.fetch_add(1, Ordering::Relaxed);
        self.id_prefix.wrapping_add(counter)
    }

    #[inline]
    /// Format ID as hex string (faster than UUID formatting)
    fn format_id(&self, id: u64) -> String {
        format!("{:016x}", id)
    }

    /// Acquire `lease://{realm}/{area}/{resource}` (synchronous, returns error if busy).
    /// Leases are namespaced by route_family to prevent cross-realm access.
    /// Full-path interning eliminates repeated parsing and hashing.
    pub fn acquire(
        &self,
        rf: RouteFamilyId,
        key: &str,
        ttl_secs: u32,
    ) -> Result<LeaseGrant, String> {
        if ttl_secs == 0 {
            return Err("invalid_ttl".into());
        }

        // Parse and intern key components once
        let lease_key = self.parse_and_intern_key(rf, key)?;
        let shard = self.pick_shard(lease_key);

        // Single flat map lookup with composite key
        let lease_lock = shard
            .entry(lease_key)
            .or_insert_with(|| Arc::new(RwLock::new(LeaseEntry::free())))
            .clone();

        let mut lease = lease_lock.write();
        let now = Instant::now();

        if lease.is_active(now) {
            return Err("lease_busy".into());
        }

        // Fast inline ID generation
        let id = self.new_id();
        let expiry_instant = now + Duration::from_secs(ttl_secs as u64);
        let expiry_unix = now_unix_secs() + ttl_secs as u64;

        // Fast SipHash token (replaces HMAC)
        let token = self.compute_token_siphash(lease_key, id, expiry_unix);
        let id_str = self.format_id(id);

        lease.id = id_str.clone();
        lease.token = token.clone();
        lease.expiry = expiry_instant;
        let body = lease.body.clone();

        Ok(LeaseGrant {
            id: id_str,
            body,
            token,
            ttl_secs,
        })
    }

    /// Extend by `add_secs`; returns remaining seconds (synchronous).
    /// Leases are namespaced by route_family to prevent cross-realm access.
    pub fn renew(
        &self,
        rf: RouteFamilyId,
        key: &str,
        id: &str,
        token: &str,
        add_secs: u32,
    ) -> Result<LeaseGrant, String> {
        if add_secs == 0 {
            return Err("invalid_ttl".into());
        }

        let lease_key = self.parse_and_intern_key(rf, key)?;
        let lease_lock = self
            .get_entry(lease_key)
            .ok_or_else(|| "lease_not_found".to_string())?;

        let mut lease = lease_lock.write();

        if !lease.is_active(Instant::now()) {
            return Err("lease_expired".into());
        }
        if lease.id != id || lease.token != token {
            return Err("invalid_token".into());
        }

        lease.expiry += Duration::from_secs(add_secs as u64);
        let new_ttl = lease
            .expiry
            .saturating_duration_since(Instant::now())
            .as_secs() as u32;

        Ok(LeaseGrant {
            id: lease.id.clone(),
            token: lease.token.clone(),
            ttl_secs: new_ttl,
            body: lease.body.clone(),
        })
    }

    /// Release; clear and prune maps (synchronous, no waiter handoff).
    /// Leases are namespaced by route_family to prevent cross-realm access.
    pub fn surrender(
        &self,
        rf: RouteFamilyId,
        key: &str,
        id: &str,
        token: &str,
    ) -> Result<(), String> {
        let lease_key = self.parse_and_intern_key(rf, key)?;
        let shard = self.pick_shard(lease_key);

        let lease_lock = match shard.get(&lease_key).map(|v| v.clone()) {
            Some(e) => e,
            None => return Err("lease_not_found".into()),
        };

        let mut lease = lease_lock.write();

        if lease.id != id || lease.token != token {
            return Err("invalid_token".into());
        }

        // Clear entry and remove from flat map
        *lease = LeaseEntry::free();
        drop(lease);
        shard.remove(&lease_key);

        Ok(())
    }
}

// === Internals ===============================================================

impl LeaseService {
    #[inline]
    /// Parse lease key and intern all components
    fn parse_and_intern_key(&self, rf: RouteFamilyId, key: &str) -> Result<LeaseKey, String> {
        let (realm, area, resource) = parse_lease_key(key)?;
        Ok(LeaseKey {
            rf,
            realm_id: self.interner.intern(realm),
            area_id: self.interner.intern(area),
            resource_id: self.interner.intern(resource),
        })
    }

    #[inline]
    /// Pick shard using composite key hash (single hash operation)
    fn pick_shard(&self, key: LeaseKey) -> &Arc<LeaseMap> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        &self.shards[(h.finish() as usize) % self.shards.len()]
    }

    #[inline]
    /// Fast SipHash-based token (replaces HMAC-SHA256)
    fn compute_token_siphash(&self, key: LeaseKey, id: u64, expiry_unix: u64) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let mut hasher = DefaultHasher::new();
        hasher.write_u64(self.token_key0);
        hasher.write_u64(self.token_key1);
        hasher.write_u32(key.rf);
        hasher.write_u32(key.realm_id);
        hasher.write_u32(key.area_id);
        hasher.write_u32(key.resource_id);
        hasher.write_u64(id);
        hasher.write_u64(expiry_unix);

        format!("{:016x}", hasher.finish())
    }

    #[inline]
    fn get_entry(&self, key: LeaseKey) -> Option<LeaseLock> {
        let shard = self.pick_shard(key);
        shard.get(&key).map(|v| v.clone())
    }
}

// === Key parsing =============================================================

#[inline]
fn parse_lease_key(key: &str) -> Result<(&str, &str, &str), String> {
    // Format: lease://{realm}/{area}/{resource}
    const PREFIX: &str = "lease://";
    if !key.starts_with(PREFIX) {
        return Err("invalid_key".into());
    }
    let rest = &key[PREFIX.len()..];
    let mut it = rest.splitn(3, '/');
    let realm = it.next().ok_or("invalid_key")?;
    let area = it.next().ok_or("invalid_key")?;
    let resource = it.next().ok_or("invalid_key")?;
    if realm.is_empty() || area.is_empty() || resource.is_empty() {
        return Err("invalid_key".into());
    }
    Ok((realm, area, resource))
}

#[inline]
fn now_unix_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::DEFAULT_RF;

    // Override LeaseService::new() in tests to disable background expirer
    fn new_test_service() -> Arc<LeaseService> {
        LeaseService::new_for_test()
    }

    #[test]
    fn should_acquire_lease_successfully() {
        // Arrange
        let svc = new_test_service();

        // Act
        let grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res1", 2)
            .unwrap();

        // Assert
        assert!(!grant.id.is_empty());
        assert!(!grant.token.is_empty());
        assert_eq!(grant.ttl_secs, 2);
        assert!(grant.body.is_none());
    }

    #[test]
    fn should_reject_acquire_with_zero_ttl() {
        // Arrange
        let svc = new_test_service();

        // Act
        let result = svc.acquire(DEFAULT_RF, "lease://realm1/area1/res", 0);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_ttl");
    }

    #[test]
    fn should_return_busy_when_lease_already_held() {
        // Arrange
        let svc = new_test_service();
        let _grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res", 5)
            .unwrap();

        // Act
        let result = svc.acquire(DEFAULT_RF, "lease://realm1/area1/res", 5);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_busy");
    }

    #[test]
    fn should_extend_active_lease() {
        // Arrange
        let svc = new_test_service();
        let grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res2", 2)
            .unwrap();

        // Act
        let added = 10u32;
        let remaining = svc
            .renew(
                DEFAULT_RF,
                "lease://realm1/area1/res2",
                &grant.id,
                &grant.token,
                added,
            )
            .unwrap();

        // Assert
        assert!(
            remaining.ttl_secs >= added,
            "remaining should be at least the added seconds"
        );
    }

    #[test]
    fn should_reject_extend_with_zero_add_secs() {
        // Arrange
        let svc = new_test_service();
        let grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res", 5)
            .unwrap();

        // Act
        let result = svc.renew(
            DEFAULT_RF,
            "lease://realm1/area1/res",
            &grant.id,
            &grant.token,
            0,
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_ttl");
    }

    #[test]
    fn should_reject_extend_with_wrong_id() {
        // Arrange
        let svc = new_test_service();
        let grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res", 5)
            .unwrap();

        // Act
        let result = svc.renew(
            DEFAULT_RF,
            "lease://realm1/area1/res",
            "wrong-id",
            &grant.token,
            5,
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");
    }

    #[test]
    fn should_reject_extend_with_wrong_token() {
        // Arrange
        let svc = new_test_service();
        let grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res", 5)
            .unwrap();

        // Act
        let result = svc.renew(
            DEFAULT_RF,
            "lease://realm1/area1/res",
            &grant.id,
            "wrong-token",
            5,
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");
    }

    #[test]
    fn should_reject_extend_for_nonexistent_lease() {
        // Arrange
        let svc = new_test_service();

        // Act
        let result = svc.renew(
            DEFAULT_RF,
            "lease://realm1/area1/no-lease",
            "fake-id",
            "fake-token",
            5,
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_not_found");
    }

    #[test]
    fn should_release_lease_successfully() {
        // Arrange
        let svc = new_test_service();
        let grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res", 5)
            .unwrap();

        // Act
        let result = svc.surrender(
            DEFAULT_RF,
            "lease://realm1/area1/res",
            &grant.id,
            &grant.token,
        );

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_remove_lease_after_release() {
        // Arrange
        let svc = new_test_service();
        let grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res", 5)
            .unwrap();
        svc.surrender(
            DEFAULT_RF,
            "lease://realm1/area1/res",
            &grant.id,
            &grant.token,
        )
        .unwrap();

        // Act
        let result = svc.acquire(DEFAULT_RF, "lease://realm1/area1/res", 5);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_release_with_wrong_id() {
        // Arrange
        let svc = new_test_service();
        let grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res", 5)
            .unwrap();

        // Act
        let result = svc.surrender(
            DEFAULT_RF,
            "lease://realm1/area1/res",
            "wrong-id",
            &grant.token,
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");
    }

    #[test]
    fn should_not_remove_lease_when_release_fails_with_wrong_id() {
        // Arrange
        let svc = new_test_service();
        let grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res", 5)
            .unwrap();
        let _ = svc.surrender(
            DEFAULT_RF,
            "lease://realm1/area1/res",
            "wrong-id",
            &grant.token,
        );

        // Act
        let result = svc.acquire(DEFAULT_RF, "lease://realm1/area1/res", 5);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_busy");
    }

    #[test]
    fn should_reject_release_with_wrong_token() {
        // Arrange
        let svc = new_test_service();
        let grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res", 5)
            .unwrap();

        // Act
        let result = svc.surrender(
            DEFAULT_RF,
            "lease://realm1/area1/res",
            &grant.id,
            "wrong-token",
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");
    }

    #[test]
    fn should_not_remove_lease_when_release_fails_with_wrong_token() {
        // Arrange
        let svc = new_test_service();
        let grant = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/res", 5)
            .unwrap();
        let _ = svc.surrender(
            DEFAULT_RF,
            "lease://realm1/area1/res",
            &grant.id,
            "wrong-token",
        );

        // Act
        let result = svc.acquire(DEFAULT_RF, "lease://realm1/area1/res", 5);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_busy");
    }

    #[test]
    fn should_reject_release_for_nonexistent_lease() {
        // Arrange
        let svc = new_test_service();

        // Act
        let result = svc.surrender(
            DEFAULT_RF,
            "lease://realm1/area1/no-lease",
            "fake-id",
            "fake-token",
        );

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_not_found");
    }

    #[test]
    fn should_handle_multiple_keys_independently() {
        // Arrange
        let svc = new_test_service();

        // Act
        let grant1 = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/key1", 5)
            .unwrap();
        let grant2 = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/key2", 5)
            .unwrap();

        // Assert
        assert_ne!(grant1.id, grant2.id);
    }

    #[test]
    fn should_acquire_two_independent_leases() {
        // Arrange
        let svc = new_test_service();
        let grant1 = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/key1", 5)
            .unwrap();
        let grant2 = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/key2", 5)
            .unwrap();

        // Act
        let renew1 = svc.renew(
            DEFAULT_RF,
            "lease://realm1/area1/key1",
            &grant1.id,
            &grant1.token,
            5,
        );
        let renew2 = svc.renew(
            DEFAULT_RF,
            "lease://realm1/area1/key2",
            &grant2.id,
            &grant2.token,
            5,
        );

        // Assert
        assert!(renew1.is_ok());
        assert!(renew2.is_ok());
    }

    #[test]
    fn should_release_one_lease_without_affecting_other() {
        // Arrange
        let svc = new_test_service();
        let grant1 = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/key1", 5)
            .unwrap();
        let grant2 = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/key2", 5)
            .unwrap();

        // Act
        svc.surrender(
            DEFAULT_RF,
            "lease://realm1/area1/key1",
            &grant1.id,
            &grant1.token,
        )
        .unwrap();

        // Assert
        let reacquire1 = svc.acquire(DEFAULT_RF, "lease://realm1/area1/key1", 5);
        let try_acquire2 = svc.acquire(DEFAULT_RF, "lease://realm1/area1/key2", 5);

        assert!(reacquire1.is_ok());
        assert!(try_acquire2.is_err());
        assert_eq!(try_acquire2.unwrap_err(), "lease_busy");

        // Verify key2 still responds to renew
        let renew2 = svc.renew(
            DEFAULT_RF,
            "lease://realm1/area1/key2",
            &grant2.id,
            &grant2.token,
            5,
        );
        assert!(renew2.is_ok());
    }

    // --- Multi-realm/route-family isolation tests ---

    #[test]
    fn should_isolate_leases_between_different_route_families() {
        // Arrange
        let svc = new_test_service();
        let rf1: RouteFamilyId = 1;
        let rf2: RouteFamilyId = 2;

        // Act
        let grant1 = svc
            .acquire(rf1, "lease://realm1/area1/resource1", 10)
            .unwrap();
        let grant2 = svc
            .acquire(rf2, "lease://realm1/area1/resource1", 10)
            .unwrap();

        // Assert
        assert_ne!(grant1.id, grant2.id);
        assert_ne!(grant1.token, grant2.token);
    }

    #[test]
    fn should_prevent_cross_realm_renew() {
        // Arrange
        let svc = new_test_service();
        let rf1: RouteFamilyId = 1;
        let rf2: RouteFamilyId = 2;
        let key = "lease://realm1/area1/resource1";

        let grant = svc.acquire(rf1, key, 10).unwrap();

        // Act
        let result = svc.renew(rf2, key, &grant.id, &grant.token, 5);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_not_found");
    }

    #[test]
    fn should_prevent_cross_realm_surrender() {
        // Arrange
        let svc = new_test_service();
        let rf1: RouteFamilyId = 1;
        let rf2: RouteFamilyId = 2;
        let key = "lease://realm1/area1/resource1";

        let grant = svc.acquire(rf1, key, 10).unwrap();

        // Act
        let result = svc.surrender(rf2, key, &grant.id, &grant.token);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_not_found");
    }

    #[test]
    fn should_allow_same_resource_across_multiple_route_families() {
        // Arrange
        let svc = new_test_service();
        let rf1: RouteFamilyId = 1;
        let rf2: RouteFamilyId = 2;
        let rf3: RouteFamilyId = 3;
        let key = "lease://realm1/area1/database";

        // Act
        let grant1 = svc.acquire(rf1, key, 5).unwrap();
        let grant2 = svc.acquire(rf2, key, 5).unwrap();
        let grant3 = svc.acquire(rf3, key, 5).unwrap();

        // Assert
        assert_ne!(grant1.id, grant2.id);
        assert_ne!(grant2.id, grant3.id);
        assert_ne!(grant1.id, grant3.id);

        // Verify each realm can renew their own lease
        let renew1 = svc.renew(rf1, key, &grant1.id, &grant1.token, 2);
        let renew2 = svc.renew(rf2, key, &grant2.id, &grant2.token, 2);
        let renew3 = svc.renew(rf3, key, &grant3.id, &grant3.token, 2);

        assert!(renew1.is_ok());
        assert!(renew2.is_ok());
        assert!(renew3.is_ok());
    }

    #[test]
    fn should_not_block_across_route_families() {
        // Arrange
        let svc = new_test_service();
        let rf1: RouteFamilyId = 1;
        let rf2: RouteFamilyId = 2;
        let key = "lease://realm1/area1/resource1";

        let grant_rf1 = svc.acquire(rf1, key, 10).unwrap();

        // Act
        let result_rf2 = svc.acquire(rf2, key, 5);

        // Assert
        assert!(result_rf2.is_ok());
        let grant_rf2 = result_rf2.unwrap();
        assert_ne!(grant_rf1.id, grant_rf2.id);

        // Cleanup
        let _ = svc.surrender(rf1, key, &grant_rf1.id, &grant_rf1.token);
        let _ = svc.surrender(rf2, key, &grant_rf2.id, &grant_rf2.token);
    }

    #[test]
    fn should_handle_many_route_families_independently() {
        // Arrange
        let svc = new_test_service();
        let num_tenants = 10;
        let key = "lease://realm1/area1/shared_resource";

        // Act
        let mut grants = Vec::new();
        for rf in 0..num_tenants {
            let grant = svc.acquire(rf, key, 30).unwrap();
            grants.push((rf, grant));
        }

        // Assert
        for i in 0..grants.len() {
            for j in (i + 1)..grants.len() {
                assert_ne!(grants[i].1.id, grants[j].1.id);
                assert_ne!(grants[i].1.token, grants[j].1.token);
            }
        }

        // Verify each lease can be renewed independently
        for (rf, grant) in &grants {
            let renew = svc.renew(*rf, key, &grant.id, &grant.token, 5);
            assert!(renew.is_ok(), "realm {} should be able to renew", rf);
        }

        // Cleanup
        for (rf, grant) in grants {
            let _ = svc.surrender(rf, key, &grant.id, &grant.token);
        }
    }
}
