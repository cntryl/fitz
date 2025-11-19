use std::sync::Arc;
use std::time::{Duration, Instant};

use super::types::*; // LeaseEntry, LeaseLock, etc.
use crate::routing::RouteFamilyId;
use base64::{engine::general_purpose, Engine as _};
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

/// Per-tenant lease map within a shard:
///   key: full lease key ("lease://realm/area/resource")
///   val: LeaseLock (Arc<RwLock<LeaseEntry>>)
type TenantMap = DashMap<String, LeaseLock>;

/// A shard owns a set of tenants (route families), each with its own lease map.
/// We still shard by (rf, realm) to keep contention low.
#[derive(Debug)]
struct Shard {
    tenants: DashMap<RouteFamilyId, Arc<TenantMap>>,
}

impl Shard {
    fn new() -> Self {
        Self {
            tenants: DashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct LeaseService {
    shards: Vec<Arc<Shard>>, // sharded by (rf, realm)
    secret: Arc<Vec<u8>>,    // HMAC key
}

// === Public API =============================================================

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
    /// UUID string generation with a single allocation.
    fn new_uuid_string(&self) -> String {
        let mut s = String::with_capacity(36);
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

        // Validate key and extract realm for sharding (no allocations here).
        let (realm, _area, _resource) = parse_lease_key(key)?;

        let shard = self.pick_shard(rf, realm);

        // Get or create the tenant map for this route family.
        let tenant = shard
            .tenants
            .entry(rf)
            .or_insert_with(|| Arc::new(TenantMap::new()))
            .clone();

        // Single map lookup: key is the *full* lease key string.
        let lease_lock = tenant
            .entry(key.to_owned())
            .or_insert_with(|| Arc::new(parking_lot::RwLock::new(LeaseEntry::free())))
            .clone();

        let mut lease = lease_lock.write();
        let now = Instant::now();

        if lease.is_active(now) {
            // Busy → return immediately, no waiter logic in sync model.
            return Err("lease_busy".into());
        }

        // Free/expired → take lease.
        let id = self.new_uuid_string();
        let expiry_instant = now + Duration::from_secs(ttl_secs as u64);

        // Fast token path: avoid extra Instant plumbing; just use "now_unix + ttl"
        let expiry_unix = now_unix_secs() + ttl_secs as u64;
        let token = self.compute_token_unix(key, &id, expiry_unix);

        lease.id = id.clone();
        lease.token = token.clone();
        lease.expiry = expiry_instant;
        let body = lease.body.clone(); // preserve existing body if any

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

        let lease_lock = self
            .get_entry(rf, key)
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

        // Note: token stays stable for the life of the lease; only expiry moves.
        Ok(LeaseGrant {
            id: lease.id.clone(),
            token: lease.token.clone(),
            ttl_secs: new_ttl,
            body: lease.body.clone(),
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
        let (realm, _area, _resource) = parse_lease_key(key)?;
        let shard = self.pick_shard(rf, realm);

        // Fast lookups: shard → tenant → lease entry.
        let tenant_arc = match shard.tenants.get(&rf).map(|v| v.clone()) {
            Some(t) => t,
            None => return Err("lease_not_found".into()),
        };

        let lease_lock = match tenant_arc.get(key).map(|v| v.clone()) {
            Some(e) => e,
            None => return Err("lease_not_found".into()),
        };

        let mut lease = lease_lock.write();

        if lease.id != id || lease.token != token {
            return Err("invalid_token".into());
        }

        // Clear entry and drop; then prune the map if empty to bound memory.
        *lease = LeaseEntry::free();
        drop(lease);

        tenant_arc.remove(key);
        if tenant_arc.is_empty() {
            shard.tenants.remove(&rf);
        }

        Ok(())
    }
}

// === Optional sync-only helpers for benches =================================

impl LeaseService {
    /// Synchronous version of token generation for benchmarking.
    /// Keeps the old Instant-based signature but routes to the fast Unix path.
    pub fn bench_token_generation(&self, key: &str, id: &str, expiry: Instant) -> String {
        let expiry_unix =
            now_unix_secs() + expiry.saturating_duration_since(Instant::now()).as_secs();
        self.compute_token_unix(key, id, expiry_unix)
    }

    /// Synchronous version of lease state transitions for micro-benchmarks.
    pub fn bench_lease_state_transitions(&self) -> u32 {
        use std::sync::Mutex;

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

        // acquire
        {
            let mut lock = entry.lock().unwrap();
            lock.id = "id".to_string();
            lock.token = "token".to_string();
            lock.expiry = now + Duration::from_secs(30);
            transitions += 1;
        }

        // check active
        {
            let lock = entry.lock().unwrap();
            let _ = lock.is_active(now);
            transitions += 1;
        }

        // expire/reset
        {
            let mut lock = entry.lock().unwrap();
            *lock = SyncLeaseEntry::free();
            transitions += 1;
        }

        transitions
    }

    pub fn bench_uuid_generation(&self) -> String {
        self.new_uuid_string()
    }
}

// === Internals ===============================================================

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
    fn compute_token_unix(&self, key: &str, id: &str, expiry_unix: u64) -> String {
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
        let (realm, _area, _resource) = parse_lease_key(key).ok()?;
        let shard = self.pick_shard(rf, realm);
        let tenant = shard.tenants.get(&rf)?.clone();
        let entry = tenant.get(key)?.clone();
        Some(entry)
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

    // --- Multi-tenant/route-family isolation tests ---

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
    fn should_prevent_cross_tenant_renew() {
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
    fn should_prevent_cross_tenant_surrender() {
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
            assert!(renew.is_ok(), "tenant {} should be able to renew", rf);
        }

        // Cleanup
        for (rf, grant) in grants {
            let _ = svc.surrender(rf, key, &grant.id, &grant.token);
        }
    }
}
