use std::sync::Arc;
use std::time::{Duration, Instant};

use super::types::*;
use base64::{engine::general_purpose, Engine as _};
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::sync::{oneshot, RwLock};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

// --- Hierarchy: Shard -> Realm -> Area -> Resource(LeaseEntry) ---

// aliases moved to `types.rs`

struct Shard {
    realms: RealmMap, // realm -> areas
}
impl Shard {
    fn new() -> Self {
        Self {
            realms: DashMap::new(),
        }
    }
}

pub struct LeaseService {
    shards: Vec<Arc<Shard>>,      // sharded by realm
    secret: Arc<Vec<u8>>,         // HMAC key
    sweep_every: Duration,        // expirer cadence
    acquire_timeout: Duration,    // max wait time for acquire when lease is busy
}

// --- Public API ---
impl LeaseService {
    pub fn new() -> Arc<Self> {
        // Allow env var override to disable expirer for harnesses/benches.
        // If FITZ_LEASE_SPAWN_EXPIRER is set to "0" or "false" (case-insensitive)
        // we won't spawn the per-shard expirer. Default is to spawn.
        let spawn = std::env::var("FITZ_LEASE_SPAWN_EXPIRER")
            .map(|v| {
                let v = v.to_ascii_lowercase();
                !(v == "0" || v == "false")
            })
            .unwrap_or(true);
        Self::new_inner(spawn)
    }

    /// Create a service but do not spawn the per-shard background expirer.
    ///
    /// Useful for microbenchmarks and other situations where a quiescent
    /// service is required (no background tasks scanning the maps).
    pub fn new_no_expirer() -> Arc<Self> {
        Self::new_inner(false)
    }

    #[cfg(test)]
    fn new_for_test() -> Arc<Self> {
        Self::new_inner(false)
    }

    fn new_inner(spawn_expirer: bool) -> Arc<Self> {
        Self::new_with_timeout(spawn_expirer, Duration::from_secs(10))
    }

    fn new_with_timeout(spawn_expirer: bool, acquire_timeout: Duration) -> Arc<Self> {
        // Clamp timeout between 0 and 20 seconds
        let clamped_timeout = acquire_timeout.min(Duration::from_secs(20));
        
        let shard_count = std::cmp::max(4, num_cpus::get()); // CPU-scaled, stable
        let svc = Arc::new(Self {
            shards: (0..shard_count).map(|_| Arc::new(Shard::new())).collect(),
            secret: Arc::new(Uuid::new_v4().as_bytes().to_vec()),
            sweep_every: Duration::from_millis(100),
            acquire_timeout: clamped_timeout,
        });

        if spawn_expirer {
            for shard in &svc.shards {
                let svc2 = svc.clone();
                let shard2 = shard.clone();
                tokio::spawn(async move { svc2.expirer(shard2).await });
            }
        }
        svc
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

    /// Acquire `lease://{realm}/{area}/{resource}` (FIFO if busy).
    pub async fn acquire(&self, key: &str, ttl_secs: u32) -> Result<LeaseGrant, String> {
        if ttl_secs == 0 {
            return Err("invalid_ttl".into());
        }
        let (realm, area, resource) = parse_lease_key(key)?;

        // Allocate owned strings once and reuse for DashMap insert/lookups to
        // avoid repeated temporary `String` allocations.
        let realm_s = realm.to_string();
        let area_s = area.to_string();
        let resource_s = resource.to_string();

        let shard = self.pick_shard(&realm_s);
        let area_map = shard
            .realms
            .entry(realm_s.clone())
            .or_insert_with(|| Arc::new(AreaMap::new()))
            .clone();
        let res_map = area_map
            .entry(area_s.clone())
            .or_insert_with(|| Arc::new(ResourceMap::new()))
            .clone();
        let entry = res_map
            .entry(resource_s.clone())
            .or_insert_with(|| Arc::new(RwLock::new(LeaseEntry::free())))
            .clone();

        let mut lock = entry.write().await;
        let now = Instant::now();

        if lock.is_active(now) {
            // busy → enqueue waiter and await with timeout
            let (tx, rx) = oneshot::channel();
            lock.waiters.push_back(Pending {
                requested_ttl: ttl_secs,
                responder: tx,
            });
            drop(lock);
            
            match tokio::time::timeout(self.acquire_timeout, rx).await {
                Ok(Ok(result)) => return result,
                Ok(Err(_)) => return Err("internal_error".into()),
                Err(_) => return Err("lease_busy_timeout".into()),
            }
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

    /// Extend by `add_secs`; returns remaining seconds.
    pub async fn renew(
        &self,
        key: &str,
        id: &str,
        token: &str,
        add_secs: u32,
    ) -> Result<u32, String> {
        if add_secs == 0 {
            return Err("invalid_ttl".into());
        }
        let entry = self
            .get_entry(key)
            .ok_or_else(|| "lease_not_found".to_string())?;
        let mut lock = entry.write().await;

        if !lock.is_active(Instant::now()) {
            return Err("lease_expired".into());
        }
        if lock.id != id || lock.token != token {
            return Err("invalid_token".into());
        }

        lock.expiry += Duration::from_secs(add_secs as u64);
        Ok(lock
            .expiry
            .saturating_duration_since(Instant::now())
            .as_secs() as u32)
    }

    /// Release; FIFO handoff if waiters exist, else clear (and prune maps).
    pub async fn release(&self, key: &str, id: &str, token: &str) -> Result<(), String> {
        let (realm, area, resource) = parse_lease_key(key)?;
        let shard = self.pick_shard(realm);

        let area_map = match shard.realms.get(realm).map(|v| v.clone()) {
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

        let mut lock = entry.write().await;

        if lock.id != id || lock.token != token {
            return Err("invalid_token".into());
        }

        if let Some(mut p) = lock.waiters.pop_front() {
            // handoff: skip any waiters that have dropped their receiver
            loop {
                let new_id = self.new_uuid_string();
                let new_expiry = Instant::now() + Duration::from_secs(p.requested_ttl as u64);
                let new_token = self.compute_token(key, &new_id, new_expiry);
                lock.id = new_id.clone();
                lock.token = new_token.clone();
                lock.expiry = new_expiry;

                // If the waiter is still listening, send the grant and stop.
                // If send fails the receiver was dropped; try the next waiter (if any).
                if p.responder
                    .send(Ok(LeaseGrant {
                        id: new_id.clone(),
                        body: lock.body.clone(),
                        token: new_token.clone(),
                        ttl_secs: p.requested_ttl,
                    }))
                    .is_ok()
                {
                    break;
                }

                // Try next waiter, if any. If none left, clear the lease and prune maps.
                match lock.waiters.pop_front() {
                    Some(next) => p = next,
                    None => {
                        *lock = LeaseEntry::free();
                        drop(lock);
                        res_map.remove(resource);
                        if res_map.is_empty() {
                            area_map.remove(area);
                        }
                        if area_map.is_empty() {
                            shard.realms.remove(realm);
                        }
                        return Ok(());
                    }
                }
            }
        } else {
            // clear & remove empty maps to keep memory bounded
            *lock = LeaseEntry::free();
            drop(lock);
            res_map.remove(resource);
            if res_map.is_empty() {
                area_map.remove(area);
            }
            if area_map.is_empty() {
                shard.realms.remove(realm);
            }
        }
        Ok(())
    }
}

// --- Internals ---
impl LeaseService {
    #[inline]
    fn pick_shard(&self, realm: &str) -> &Arc<Shard> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let mut h = DefaultHasher::new();
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

    #[inline]
    /// Compute token without allocating a full key string by supplying the
    /// lease key components separately: realm, area, resource. This avoids a
    /// temporary `String` when callers already have the parts (used by the
    /// per-shard expirer).
    fn compute_token_parts(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        id: &str,
        expiry: Instant,
    ) -> String {
        let expiry_unix = (std::time::SystemTime::now()
            + expiry.saturating_duration_since(Instant::now()))
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC key");
        mac.update(b"lease://");
        mac.update(realm.as_bytes());
        mac.update(b"/");
        mac.update(area.as_bytes());
        mac.update(b"/");
        mac.update(resource.as_bytes());
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

    fn get_entry(&self, key: &str) -> Option<LeaseLock> {
        let (realm, area, resource) = parse_lease_key(key).ok()?;
        let shard = self.pick_shard(realm);
        let area_map = shard.realms.get(realm)?.clone();
        let res_map = area_map.get(area)?.clone();
        let entry = res_map.get(resource)?.clone();
        Some(entry)
    }

    // Per-shard expirer: scans realms/areas/resources, handles expirations & handoffs.
    async fn expirer(self: Arc<Self>, shard: Arc<Shard>) {
        let tick = self.sweep_every;
        loop {
            let now = Instant::now();

            for realm_kv in shard.realms.iter() {
                let realm = realm_kv.key().clone();
                let areas = realm_kv.value().clone();

                for area_kv in areas.iter() {
                    let area = area_kv.key().clone();
                    let resources = area_kv.value().clone();

                    for res_kv in resources.iter() {
                        let res = res_kv.key().clone();
                        let entry = res_kv.value().clone();

                        // Quick read check; skip if locked to avoid blocking acquire/release/extend
                        let is_expired = {
                            match entry.try_read() {
                                Ok(lock) => !lock.is_active(now),
                                Err(_) => continue, // skip if locked
                            }
                        };

                        if !is_expired {
                            continue;
                        }

                        // Expired: handoff or prune (non-blocking to avoid starving acquire calls)
                        let mut lock = match entry.try_write() {
                            Ok(l) => l,
                            Err(_) => continue, // skip if locked by acquire/release/extend
                        };
                        if lock.is_active(now) {
                            continue;
                        } // raced

                        if let Some(mut p) = lock.waiters.pop_front() {
                            // Handoff loop: skip waiters that dropped their receiver
                            loop {
                                let new_id = self.new_uuid_string();
                                let new_expiry = now + Duration::from_secs(p.requested_ttl as u64);
                                // Compute token directly from components to avoid
                                // allocating a temporary `String` for the full key.
                                let new_token = self
                                    .compute_token_parts(&realm, &area, &res, &new_id, new_expiry);
                                lock.id = new_id.clone();
                                lock.token = new_token.clone();
                                lock.expiry = new_expiry;

                                if p.responder
                                    .send(Ok(LeaseGrant {
                                        id: new_id.clone(),
                                        body: lock.body.clone(),
                                        token: new_token.clone(),
                                        ttl_secs: p.requested_ttl,
                                    }))
                                    .is_ok()
                                {
                                    break;
                                }

                                match lock.waiters.pop_front() {
                                    Some(next) => p = next,
                                    None => {
                                        *lock = LeaseEntry::free();
                                        drop(lock);
                                        resources.remove(&res);
                                        if resources.is_empty() {
                                            areas.remove(&area);
                                        }
                                        if areas.is_empty() {
                                            shard.realms.remove(&realm);
                                        }
                                        break;
                                    }
                                }
                            }
                        } else {
                            *lock = LeaseEntry::free();
                            drop(lock);
                            resources.remove(&res);
                            if resources.is_empty() {
                                areas.remove(&area);
                            }
                            if areas.is_empty() {
                                shard.realms.remove(&realm);
                            }
                        }
                    }
                }
            }

            tokio::time::sleep(tick).await;
        }
    }
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
    use tokio::time::{sleep, Duration};

    // Override LeaseService::new() in tests to disable background expirer
    fn new_test_service() -> Arc<LeaseService> {
        LeaseService::new_for_test()
    }

    #[test]
    fn should_clamp_acquire_timeout_to_max_20_seconds() {
        // Arrange
        let excessive_timeout = Duration::from_secs(100);

        // Act
        let svc = LeaseService::new_with_timeout(false, excessive_timeout);

        // Assert
        assert_eq!(svc.acquire_timeout, Duration::from_secs(20));
    }

    #[test]
    fn should_allow_zero_second_timeout() {
        // Arrange
        let zero_timeout = Duration::from_secs(0);

        // Act
        let svc = LeaseService::new_with_timeout(false, zero_timeout);

        // Assert
        assert_eq!(svc.acquire_timeout, Duration::from_secs(0));
    }

    #[test]
    fn should_preserve_timeout_within_range() {
        // Arrange
        let valid_timeout = Duration::from_secs(15);

        // Act
        let svc = LeaseService::new_with_timeout(false, valid_timeout);

        // Assert
        assert_eq!(svc.acquire_timeout, Duration::from_secs(15));
    }

    #[tokio::test]
    async fn should_acquire_lease_successfully() {
        // Arrange
        let svc = new_test_service();

        // Act
        let grant = svc.acquire("lease://realm1/area1/res1", 2).await.unwrap();

        // Assert
        assert!(!grant.id.is_empty());
        assert!(!grant.token.is_empty());
        assert_eq!(grant.ttl_secs, 2);
        assert!(grant.body.is_none());
    }

    #[tokio::test]
    async fn should_reject_acquire_with_zero_ttl() {
        // Arrange
        let svc = new_test_service();

        // Act
        let result = svc.acquire("lease://realm1/area1/res", 0).await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_ttl");
    }

    #[tokio::test]
    async fn should_extend_active_lease() {
        // Arrange
        let svc = new_test_service();
        let grant = svc.acquire("lease://realm1/area1/res2", 2).await.unwrap();

        // Act
        let added = 10u32;
        let remaining = svc
            .renew("lease://realm1/area1/res2", &grant.id, &grant.token, added)
            .await
            .unwrap();

        // Assert
        assert!(
            remaining >= added,
            "remaining should be at least the added seconds"
        );
    }

    #[tokio::test]
    async fn should_reject_extend_with_zero_add_secs() {
        // Arrange
        let svc = new_test_service();
        let grant = svc.acquire("lease://realm1/area1/res", 5).await.unwrap();

        // Act
        let result = svc
            .renew("lease://realm1/area1/res", &grant.id, &grant.token, 0)
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_ttl");
    }

    #[tokio::test]
    async fn should_reject_extend_with_wrong_id() {
        // Arrange
        let svc = new_test_service();
        let grant = svc.acquire("lease://realm1/area1/res", 5).await.unwrap();

        // Act
        let result = svc
            .renew("lease://realm1/area1/res", "wrong-id", &grant.token, 5)
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");
    }

    #[tokio::test]
    async fn should_reject_extend_with_wrong_token() {
        // Arrange
        let svc = new_test_service();
        let grant = svc.acquire("lease://realm1/area1/res", 5).await.unwrap();

        // Act
        let result = svc
            .renew("lease://realm1/area1/res", &grant.id, "wrong-token", 5)
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");
    }

    #[tokio::test]
    async fn should_reject_extend_for_nonexistent_lease() {
        // Arrange
        let svc = new_test_service();

        // Act
        let result = svc
            .renew("lease://realm1/area1/no-lease", "fake-id", "fake-token", 5)
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_not_found");
    }

    #[tokio::test]
    async fn should_reject_extend_for_expired_lease() {
        // Arrange
        let svc = new_test_service();
        let grant = svc
            .acquire("lease://realm1/area1/res_expired_test", 1)
            .await
            .unwrap();

        // wait for lease to expire (expiration task may clean it up)
        sleep(Duration::from_secs(2)).await;

        // Act
        let result = svc
            .renew(
                "lease://realm1/area1/res_expired_test",
                &grant.id,
                &grant.token,
                5,
            )
            .await;

        // Assert - could be either lease_expired or lease_not_found depending on timing
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err == "lease_expired" || err == "lease_not_found",
            "expected lease_expired or lease_not_found, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn should_release_lease_successfully() {
        // Arrange
        let svc = new_test_service();
        let grant = svc.acquire("lease://realm1/area1/res", 5).await.unwrap();

        // Act
        let result = svc
            .release("lease://realm1/area1/res", &grant.id, &grant.token)
            .await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_remove_lease_after_release() {
        // Arrange
        let svc = new_test_service();
        let grant = svc.acquire("lease://realm1/area1/res", 5).await.unwrap();
        svc.release("lease://realm1/area1/res", &grant.id, &grant.token)
            .await
            .unwrap();

        // Act - try to acquire again
        let result = svc.acquire("lease://realm1/area1/res", 5).await;

        // Assert - should succeed because lease was removed
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_reject_release_with_wrong_id() {
        // Arrange
        let svc = new_test_service();
        let grant = svc.acquire("lease://realm1/area1/res", 5).await.unwrap();

        // Act
        let result = svc
            .release("lease://realm1/area1/res", "wrong-id", &grant.token)
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");
    }

    #[tokio::test]
    async fn should_not_remove_lease_when_release_fails_with_wrong_id() {
        // Arrange
        let svc = new_test_service();
        let grant = svc.acquire("lease://realm1/area1/res", 5).await.unwrap();
        let _ = svc
            .release("lease://realm1/area1/res", "wrong-id", &grant.token)
            .await;

        // Act - try to acquire again (with timeout since it should block)
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            svc.acquire("lease://realm1/area1/res", 5)
        ).await;

        // Assert - should timeout because lease still exists
        assert!(result.is_err(), "acquire should timeout/block because lease is still held");
    }

    #[tokio::test]
    async fn should_reject_release_with_wrong_token() {
        // Arrange
        let svc = new_test_service();
        let grant = svc.acquire("lease://realm1/area1/res", 5).await.unwrap();

        // Act
        let result = svc
            .release("lease://realm1/area1/res", &grant.id, "wrong-token")
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");
    }

    #[tokio::test]
    async fn should_not_remove_lease_when_release_fails_with_wrong_token() {
        // Arrange
        let svc = new_test_service();
        let grant = svc.acquire("lease://realm1/area1/res", 5).await.unwrap();
        let _ = svc
            .release("lease://realm1/area1/res", &grant.id, "wrong-token")
            .await;

        // Act - try to acquire again (with timeout since it should block)
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            svc.acquire("lease://realm1/area1/res", 5)
        ).await;

        // Assert - should timeout because lease still exists
        assert!(result.is_err(), "acquire should timeout/block because lease is still held");
    }

    #[tokio::test]
    async fn should_reject_release_for_nonexistent_lease() {
        // Arrange
        let svc = new_test_service();

        // Act
        let result = svc
            .release("lease://realm1/area1/no-lease", "fake-id", "fake-token")
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_not_found");
    }

    #[tokio::test]
    async fn should_enqueue_waiter_when_lease_busy() {
        // Arrange
        let svc = new_test_service();
        let _holder = svc.acquire("lease://realm1/area1/res", 10).await.unwrap();

        // spawn a second acquire that should wait
        let svc_clone = svc.clone();
        let waiter =
            tokio::spawn(async move { svc_clone.acquire("lease://realm1/area1/res", 5).await });

        // give waiter time to enqueue
        sleep(Duration::from_millis(50)).await;

        // Act & Assert
        assert!(!waiter.is_finished());
    }

    #[tokio::test]
    async fn should_grant_lease_to_waiter_on_release() {
        // Arrange
        let svc = new_test_service();
        let holder = svc.acquire("lease://realm1/area1/res3", 5).await.unwrap();

        // spawn a waiter that will block until the lease is released
        let svc_clone = svc.clone();
        let waiter =
            tokio::spawn(async move { svc_clone.acquire("lease://realm1/area1/res3", 3).await });

        // give the waiter a moment to enqueue
        sleep(Duration::from_millis(50)).await;

        // Act
        svc.release("lease://realm1/area1/res3", &holder.id, &holder.token)
            .await
            .unwrap();

        // Assert
        let res = waiter.await.unwrap().unwrap();
        assert!(!res.id.is_empty());
        assert_eq!(res.ttl_secs, 3);
        assert_ne!(res.id, holder.id, "waiter should get a different lease ID");
    }

    #[tokio::test]
    async fn should_grant_lease_to_next_waiter_fifo() {
        // Arrange
        let svc = new_test_service();
        let holder = svc.acquire("lease://realm1/area1/res", 5).await.unwrap();

        // spawn two waiters
        let svc1 = svc.clone();
        let waiter1 =
            tokio::spawn(async move { svc1.acquire("lease://realm1/area1/res", 2).await });

        sleep(Duration::from_millis(10)).await;

        let svc2 = svc.clone();
        let _waiter2 =
            tokio::spawn(async move { svc2.acquire("lease://realm1/area1/res", 3).await });

        sleep(Duration::from_millis(50)).await;

        // Act
        svc.release("lease://realm1/area1/res", &holder.id, &holder.token)
            .await
            .unwrap();

        // Assert
        let grant1 = waiter1.await.unwrap().unwrap();
        assert_eq!(grant1.ttl_secs, 2);
    }

    #[tokio::test]
    async fn should_keep_second_waiter_waiting_after_first_granted() {
        // Arrange
        let svc = new_test_service();
        let holder = svc.acquire("lease://realm1/area1/res", 5).await.unwrap();

        // spawn two waiters
        let svc1 = svc.clone();
        let _waiter1 =
            tokio::spawn(async move { svc1.acquire("lease://realm1/area1/res", 2).await });

        sleep(Duration::from_millis(10)).await;

        let svc2 = svc.clone();
        let _waiter2 =
            tokio::spawn(async move { svc2.acquire("lease://realm1/area1/res", 3).await });

        sleep(Duration::from_millis(50)).await;

        // Act
        svc.release("lease://realm1/area1/res", &holder.id, &holder.token)
            .await
            .unwrap();

        sleep(Duration::from_millis(50)).await;

        // Assert
        assert!(
            !_waiter2.is_finished(),
            "second waiter should still be blocked"
        );
    }

    #[tokio::test]
    async fn should_expire_lease_and_grant_to_waiter() {
        // Arrange
        let svc = LeaseService::new(); // needs expirer for this test
        let _holder = svc.acquire("lease://realm1/area1/res", 1).await.unwrap();

        // spawn a waiter
        let svc_clone = svc.clone();
        let waiter =
            tokio::spawn(async move { svc_clone.acquire("lease://realm1/area1/res", 2).await });

        // Act
        sleep(Duration::from_secs(2)).await;

        // Assert
        let grant = waiter.await.unwrap().unwrap();
        assert!(!grant.id.is_empty());
        assert_eq!(grant.ttl_secs, 2);
    }

    #[tokio::test]
    async fn should_handle_multiple_keys_independently() {
        // Arrange
        let svc = new_test_service();

        // Act
        let grant1 = svc.acquire("lease://realm1/area1/key1", 5).await.unwrap();
        let grant2 = svc.acquire("lease://realm1/area1/key2", 5).await.unwrap();

        // Assert
        assert_ne!(grant1.id, grant2.id);
    }

    #[tokio::test]
    async fn should_acquire_two_independent_leases() {
        // Arrange
        let svc = new_test_service();
        let grant1 = svc.acquire("lease://realm1/area1/key1", 5).await.unwrap();
        let grant2 = svc.acquire("lease://realm1/area1/key2", 5).await.unwrap();

        // Act - verify by trying to renew each
        let renew1 = svc.renew("lease://realm1/area1/key1", &grant1.id, &grant1.token, 5).await;
        let renew2 = svc.renew("lease://realm1/area1/key2", &grant2.id, &grant2.token, 5).await;

        // Assert
        assert!(renew1.is_ok());
        assert!(renew2.is_ok());
    }

    #[tokio::test]
    async fn should_release_one_lease_without_affecting_other() {
        // Arrange
        let svc = new_test_service();
        let grant1 = svc.acquire("lease://realm1/area1/key1", 5).await.unwrap();
        let grant2 = svc.acquire("lease://realm1/area1/key2", 5).await.unwrap();

        // Act
        svc.release("lease://realm1/area1/key1", &grant1.id, &grant1.token)
            .await
            .unwrap();

        // Assert - key1 can be reacquired, key2 still held (timeout since blocked)
        let reacquire1 = svc.acquire("lease://realm1/area1/key1", 5).await;
        let try_acquire2 = tokio::time::timeout(
            Duration::from_millis(100),
            svc.acquire("lease://realm1/area1/key2", 5)
        ).await;
        assert!(reacquire1.is_ok());
        assert!(try_acquire2.is_err(), "acquire of key2 should timeout because it's still held");
        
        // Verify key2 still responds to renew
        let renew2 = svc.renew("lease://realm1/area1/key2", &grant2.id, &grant2.token, 5).await;
        assert!(renew2.is_ok());
    }

    #[tokio::test]
    async fn should_allow_reacquire_after_expiration() {
        // Arrange
        let svc = new_test_service();
        let grant1 = svc
            .acquire("lease://realm1/area1/res_reacquire_test", 1)
            .await
            .unwrap();

        // wait for expiration
        sleep(Duration::from_secs(2)).await;

        // Act
        let grant2 = svc
            .acquire("lease://realm1/area1/res_reacquire_test", 5)
            .await
            .unwrap();

        // Assert
        assert_ne!(grant1.id, grant2.id, "new lease should have different ID");
        assert_ne!(
            grant1.token, grant2.token,
            "new lease should have different token"
        );
    }
}
