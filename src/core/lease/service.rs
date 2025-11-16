use std::sync::Arc;
use std::time::{Duration, Instant};

use super::types::*;
use crate::routing::RouteFamilyId;
use base64::{engine::general_purpose, Engine as _};
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

// --- Hierarchy: Shard -> RouteFamilyMap -> Realm -> Area -> Resource(LeaseEntry) ---
// Leases are now namespaced by route_family (tenant) to prevent cross-tenant access

// aliases moved to `types.rs`

#[derive(Debug)]
struct Shard {
    route_families: DashMap<RouteFamilyId, Arc<RealmMap>>, // rf -> realms
}
impl Shard {
    fn new() -> Self {
        Self {
            route_families: DashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct LeaseService {
    shards: Vec<Arc<Shard>>, // sharded by realm
    secret: Arc<Vec<u8>>,    // HMAC key
}

// --- Public API ---
impl LeaseService {
    pub fn new() -> Arc<Self> {
        let shard_count = std::cmp::max(4, num_cpus::get()); // CPU-scaled, stable
        Arc::new(Self {
            shards: (0..shard_count).map(|_| Arc::new(Shard::new())).collect(),
            secret: Arc::new(Uuid::new_v4().as_bytes().to_vec()),
        })
    }

    #[cfg(test)]
    fn new_for_test() -> Arc<Self> {
        Self::new()
    }

    #[inline]
    /// Generate a UUID string into a pre-allocated buffer to avoid an extra
    /// temporary allocation that `to_string()` would create.
    fn new_uuid_string(&self) -> String {
        // UUIDs are 36 chars with hyphens (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
        let mut s = String::with_capacity(36);
        // Using display formatting writes directly into the allocated String.
        use std::fmt::Write as _;
        let u = Uuid::new_v4();
        write!(&mut s, "{}", u).expect("writing uuid");
        s
    }

    /// Acquire `lease://{realm}/{area}/{resource}` (synchronous, returns error if busy).
    /// Leases are namespaced by route_family to prevent cross-tenant access.
    pub fn acquire(
        &self,
        rf: RouteFamilyId,
        key: &str,
        ttl_secs: u32,
    ) -> Result<LeaseGrant, String> {
        if ttl_secs == 0 {
            return Err("invalid_ttl".into());
        }
        let (realm, area, resource) = parse_lease_key(key)?;

        // Allocate owned strings once and reuse for DashMap insert/lookups to
        // avoid repeated temporary `String` allocations.
        let realm_s = realm.to_string();
        let area_s = area.to_string();
        let resource_s = resource.to_string();

        let shard = self.pick_shard(rf, &realm_s);
        let realm_map = shard
            .route_families
            .entry(rf)
            .or_insert_with(|| Arc::new(RealmMap::new()))
            .clone();
        let area_map = realm_map
            .entry(realm_s.clone())
            .or_insert_with(|| Arc::new(AreaMap::new()))
            .clone();
        let res_map = area_map
            .entry(area_s.clone())
            .or_insert_with(|| Arc::new(ResourceMap::new()))
            .clone();
        let entry = res_map
            .entry(resource_s.clone())
            .or_insert_with(|| Arc::new(parking_lot::RwLock::new(LeaseEntry::free())))
            .clone();

        let mut lock = entry.write();
        let now = Instant::now();

        if lock.is_active(now) {
            // busy → return error immediately (sync model, no waiting)
            return Err("lease_busy".into());
        }

        // free/expired → take lease (reuse expired lease or initialize new one)
        let id = self.new_uuid_string();
        let expiry = now + Duration::from_secs(ttl_secs as u64);
        let token = self.compute_token(key, &id, expiry);

        lock.id = id.clone();
        lock.token = token.clone();
        lock.expiry = expiry;
        // Keep existing body if any, or preserve None for new leases
        let body = lock.body.clone();

        Ok(LeaseGrant {
            id,
            body,
            token,
            ttl_secs,
        })
    }

    /// Extend by `add_secs`; returns remaining seconds (synchronous).
    /// Leases are namespaced by route_family to prevent cross-tenant access.
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
        let entry = self
            .get_entry(rf, key)
            .ok_or_else(|| "lease_not_found".to_string())?;
        let mut lock = entry.write();

        if !lock.is_active(Instant::now()) {
            return Err("lease_expired".into());
        }
        if lock.id != id || lock.token != token {
            return Err("invalid_token".into());
        }

        lock.expiry += Duration::from_secs(add_secs as u64);
        let new_ttl = lock
            .expiry
            .saturating_duration_since(Instant::now())
            .as_secs() as u32;
        
        Ok(LeaseGrant {
            id: lock.id.clone(),
            token: lock.token.clone(),
            ttl_secs: new_ttl,
            body: lock.body.clone(),
        })
    }

    /// Release; clear and prune maps (synchronous, no waiter handoff).
    /// Leases are namespaced by route_family to prevent cross-tenant access.
    pub fn surrender(
        &self,
        rf: RouteFamilyId,
        key: &str,
        id: &str,
        token: &str,
    ) -> Result<(), String> {
        let (realm, area, resource) = parse_lease_key(key)?;
        let shard = self.pick_shard(rf, realm);

        let realm_map = match shard.route_families.get(&rf).map(|v| v.clone()) {
            Some(v) => v,
            None => return Err("lease_not_found".into()),
        };
        let area_map = match realm_map.get(realm).map(|v| v.clone()) {
            Some(v) => v,
            None => return Err("lease_not_found".into()),
        };
        let res_map = match area_map.get(area).map(|v| v.clone()) {
            Some(v) => v,
            None => return Err("lease_not_found".into()),
        };
        let entry = match res_map.get(resource).map(|v| v.clone()) {
            Some(v) => v,
            None => return Err("lease_not_found".into()),
        };

        let mut lock = entry.write();

        if lock.id != id || lock.token != token {
            return Err("invalid_token".into());
        }

        // clear & remove empty maps to keep memory bounded
        *lock = LeaseEntry::free();
        drop(lock);
        res_map.remove(resource);
        if res_map.is_empty() {
            area_map.remove(area);
        }
        if area_map.is_empty() {
            realm_map.remove(realm);
        }
        if realm_map.is_empty() {
            shard.route_families.remove(&rf);
        }
        Ok(())
    }

    // `surrender` is the canonical domain term; no alias needed.
}

// --- Sync Benchmark Methods ---
// These demonstrate the core domain logic without async overhead for performance analysis

impl LeaseService {
    /// Synchronous version of token generation for benchmarking
    /// Measures pure CPU/memory operations without async runtime noise
    pub fn bench_token_generation(&self, key: &str, id: &str, expiry: Instant) -> String {
        self.compute_token(key, id, expiry)
    }

    /// Synchronous version of lease state transitions for benchmarking
    /// Demonstrates core domain logic: acquire -> check active -> expire/reset
    pub fn bench_lease_state_transitions(&self) -> u32 {
        use std::sync::Mutex;
        use std::time::Duration;

        // Use std::sync primitives for pure sync benchmarking
        struct SyncLeaseEntry {
            id: String,
            token: String,
            expiry: Instant,
        }

        impl SyncLeaseEntry {
            fn free() -> Self {
                Self {
                    id: String::new(),
                    token: String::new(),
                    expiry: Instant::now(),
                }
            }

            fn is_active(&self, now: Instant) -> bool {
                !self.id.is_empty() && now < self.expiry
            }
        }

        let entry = Mutex::new(SyncLeaseEntry::free());
        let now = Instant::now();
        let mut transitions = 0u32;

        // Simulate acquire
        {
            let mut lock = entry.lock().unwrap();
            lock.id = "id".to_string();
            lock.token = "token".to_string();
            lock.expiry = now + Duration::from_secs(30);
            transitions += 1;
        }

        // Simulate check active
        {
            let lock = entry.lock().unwrap();
            let _active = lock.is_active(now);
            transitions += 1;
        }

        // Simulate expire/reset
        {
            let mut lock = entry.lock().unwrap();
            *lock = SyncLeaseEntry::free();
            transitions += 1;
        }

        transitions
    }

    /// Synchronous version of UUID generation for benchmarking
    pub fn bench_uuid_generation(&self) -> String {
        self.new_uuid_string()
    }
}

// --- Internals ---
impl LeaseService {
    #[inline]
    /// Pick a shard based on route_family and realm for consistent sharding.
    /// route_family provides tenant isolation, realm provides distribution.
    fn pick_shard(&self, rf: RouteFamilyId, realm: &str) -> &Arc<Shard> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        rf.hash(&mut h);
        h.write(realm.as_bytes());
        &self.shards[(h.finish() as usize) % self.shards.len()]
    }

    #[inline]
    fn compute_token(&self, key: &str, id: &str, expiry: Instant) -> String {
        let expiry_unix = (std::time::SystemTime::now()
            + expiry.saturating_duration_since(Instant::now()))
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC key");
        mac.update(key.as_bytes());
        mac.update(b"|");
        mac.update(id.as_bytes());
        mac.update(b"|");
        // write digits of expiry without alloc
        let mut buf = [0u8; 20];
        let mut t = expiry_unix;
        let mut len = 0;
        if t == 0 {
            buf[0] = b'0';
            len = 1;
        } else {
            while t > 0 {
                buf[len] = b'0' + (t % 10) as u8;
                t /= 10;
                len += 1;
            }
            buf[..len].reverse();
        }
        mac.update(&buf[..len]);
        general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }

    fn get_entry(&self, rf: RouteFamilyId, key: &str) -> Option<LeaseLock> {
        let (realm, area, resource) = parse_lease_key(key).ok()?;
        let shard = self.pick_shard(rf, realm);
        let realm_map = shard.route_families.get(&rf)?.clone();
        let area_map = realm_map.get(realm)?.clone();
        let res_map = area_map.get(area)?.clone();
        let entry = res_map.get(resource)?.clone();
        Some(entry)
    }

    // Note: No background expirer in sync model. Expired leases are detected
    // and cleaned up on next acquire attempt.
}

// --- Key parsing ---

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
            remaining >= added,
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
        let result = svc
            .renew(
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
        let result = svc
            .renew(
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
        let result = svc
            .renew(
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
        let result = svc
            .renew(
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
        let result = svc
            .surrender(
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

        // Act - try to acquire again
        let result = svc.acquire(DEFAULT_RF, "lease://realm1/area1/res", 5);

        // Assert - should succeed because lease was removed
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
        let result = svc
            .surrender(
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
        let _ = svc
            .surrender(
                DEFAULT_RF,
                "lease://realm1/area1/res",
                "wrong-id",
                &grant.token,
            );

        // Act - try to acquire again
        let result = svc.acquire(DEFAULT_RF, "lease://realm1/area1/res", 5);

        // Assert - should get lease_busy because lease still exists
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
        let result = svc
            .surrender(
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
        let _ = svc
            .surrender(
                DEFAULT_RF,
                "lease://realm1/area1/res",
                &grant.id,
                "wrong-token",
            );

        // Act - try to acquire again
        let result = svc.acquire(DEFAULT_RF, "lease://realm1/area1/res", 5);

        // Assert - should get lease_busy because lease still exists
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_busy");
    }

    #[test]
    fn should_reject_release_for_nonexistent_lease() {
        // Arrange
        let svc = new_test_service();

        // Act
        let result = svc
            .surrender(
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

        // Act - verify by trying to renew each
        let renew1 = svc
            .renew(
                DEFAULT_RF,
                "lease://realm1/area1/key1",
                &grant1.id,
                &grant1.token,
                5,
            );
        let renew2 = svc
            .renew(
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

        // Assert - key1 can be reacquired, key2 still held (returns busy)
        let reacquire1 = svc
            .acquire(DEFAULT_RF, "lease://realm1/area1/key1", 5);
        let try_acquire2 = svc.acquire(DEFAULT_RF, "lease://realm1/area1/key2", 5);
        
        assert!(reacquire1.is_ok());
        assert!(try_acquire2.is_err());
        assert_eq!(try_acquire2.unwrap_err(), "lease_busy");

        // Verify key2 still responds to renew
        let renew2 = svc
            .renew(
                DEFAULT_RF,
                "lease://realm1/area1/key2",
                &grant2.id,
                &grant2.token,
                5,
            );
        assert!(renew2.is_ok());
    }

    // --- Multi-tenant/route-family isolation tests ---

    #[test]
    fn should_isolate_leases_between_different_route_families() {
        // Arrange
        let svc = new_test_service();
        let rf1: RouteFamilyId = 1;
        let rf2: RouteFamilyId = 2;

        // Act - acquire same resource in different route families
        let grant1 = svc
            .acquire(rf1, "lease://realm1/area1/resource1", 10)
            .unwrap();
        let grant2 = svc
            .acquire(rf2, "lease://realm1/area1/resource1", 10)
            .unwrap();

        // Assert - different leases
        assert_ne!(grant1.id, grant2.id);
        assert_ne!(grant1.token, grant2.token);
    }

    #[test]
    fn should_prevent_cross_tenant_renew() {
        // Arrange
        let svc = new_test_service();
        let rf1: RouteFamilyId = 1;
        let rf2: RouteFamilyId = 2;
        let key = "lease://realm1/area1/resource1";

        let grant = svc.acquire(rf1, key, 10).unwrap();

        // Act - try to renew lease from rf1 using rf2
        let result = svc.renew(rf2, key, &grant.id, &grant.token, 5);

        // Assert - should fail (lease not found in rf2)
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_not_found");
    }

    #[test]
    fn should_prevent_cross_tenant_surrender() {
        // Arrange
        let svc = new_test_service();
        let rf1: RouteFamilyId = 1;
        let rf2: RouteFamilyId = 2;
        let key = "lease://realm1/area1/resource1";

        let grant = svc.acquire(rf1, key, 10).unwrap();

        // Act - try to surrender lease from rf1 using rf2
        let result = svc.surrender(rf2, key, &grant.id, &grant.token);

        // Assert - should fail (lease not found in rf2)
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

        // Act - each tenant acquires same resource independently
        let grant1 = svc.acquire(rf1, key, 5).unwrap();
        let grant2 = svc.acquire(rf2, key, 5).unwrap();
        let grant3 = svc.acquire(rf3, key, 5).unwrap();

        // Assert - all three leases coexist without interference
        assert_ne!(grant1.id, grant2.id);
        assert_ne!(grant2.id, grant3.id);
        assert_ne!(grant1.id, grant3.id);

        // Verify each tenant can renew their own lease
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

        // Act - tenant 1 acquires resource
        let grant_rf1 = svc.acquire(rf1, key, 10).unwrap();

        // Act - tenant 2 should NOT block (different route family)
        let result_rf2 = svc.acquire(rf2, key, 5);

        // Assert - tenant 2 gets immediate lease (no wait)
        assert!(result_rf2.is_ok());
        let grant_rf2 = result_rf2.unwrap();
        assert_ne!(grant_rf1.id, grant_rf2.id);

        // Cleanup
        let _ = svc
            .surrender(rf1, key, &grant_rf1.id, &grant_rf1.token);
        let _ = svc
            .surrender(rf2, key, &grant_rf2.id, &grant_rf2.token);
    }

    #[test]
    fn should_handle_many_route_families_independently() {
        // Arrange
        let svc = new_test_service();
        let num_tenants = 10;
        let key = "lease://realm1/area1/shared_resource";

        // Act - acquire lease in many route families
        let mut grants = Vec::new();
        for rf in 0..num_tenants {
            let grant = svc.acquire(rf, key, 30).unwrap();
            grants.push((rf, grant));
        }

        // Assert - all leases are distinct
        for i in 0..grants.len() {
            for j in (i + 1)..grants.len() {
                assert_ne!(grants[i].1.id, grants[j].1.id);
                assert_ne!(grants[i].1.token, grants[j].1.token);
            }
        }

        // Verify each lease can be renewed independently
        for (rf, grant) in &grants {
            let renew = svc.renew(*rf, key, &grant.id, &grant.token, 5);
            assert!(renew.is_ok(), "tenant {} should be able to renew", rf);
        }

        // Cleanup
        for (rf, grant) in grants {
            let _ = svc.surrender(rf, key, &grant.id, &grant.token);
        }
    }
}
