//! Lease domain service - ephemeral resource locking

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::sync::{oneshot, Mutex, Notify};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

/// Configuration for LeaseService behavior
#[derive(Clone, Default)]
pub struct LeaseConfig {
    pub disable_timers: bool,
    pub token_mode: TokenMode,
    pub fast_ids: bool,  // Use sequential IDs instead of UUIDs for benches
}

/// Token generation mode
#[derive(Clone, Copy)]
pub enum TokenMode {
    Hmac,
    Fast,
}

impl Default for TokenMode {
    fn default() -> Self {
        TokenMode::Hmac
    }
}

/// Grant returned to a waiter when a lease is assigned
#[derive(Debug)]
pub struct LeaseGrant {
    pub id: String,
    pub body: Option<Vec<u8>>,
    pub token: String,
    pub ttl_secs: u32,
}

struct Pending {
    requested_ttl: u32,
    responder: oneshot::Sender<Result<LeaseGrant, String>>,
}

struct LeaseEntry {
    id: String,
    token: String,
    expiry: Instant,
    ttl_secs: u32,
    body: Option<Vec<u8>>,
    waiters: VecDeque<Pending>,
}

/// Expiry tracking item for priority queue (min-heap)
#[derive(Eq, PartialEq)]
struct ExpiryItem {
    key: String,
    expiry: Instant,
}

impl Ord for ExpiryItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap (earliest expiry has highest priority)
        other.expiry.cmp(&self.expiry).then_with(|| self.key.cmp(&other.key))
    }
}

impl PartialOrd for ExpiryItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct Inner {
    leases: HashMap<String, LeaseEntry>,
    expiry_heap: BinaryHeap<ExpiryItem>,
}

/// In-memory lease service. All data is kept in-process only.
#[derive(Clone)]
pub struct LeaseService {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Notify>,
    secret: Arc<Vec<u8>>,
    cfg: LeaseConfig,
    id_counter: Arc<AtomicU64>,  // For fast_ids mode
}

impl LeaseService {
    /// Create a new LeaseService and spawn the expiration task.
    pub fn new() -> Arc<Self> {
        Self::new_with_config(LeaseConfig::default())
    }

    /// Create a new LeaseService with custom configuration.
    pub fn new_with_config(cfg: LeaseConfig) -> Arc<Self> {
        let svc = Arc::new(Self {
            inner: Arc::new(Mutex::new(Inner {
                leases: HashMap::new(),
                expiry_heap: BinaryHeap::new(),
            })),
            notify: Arc::new(Notify::new()),
            // generate a random secret for HMAC (in-memory only)
            secret: Arc::new(Uuid::new_v4().as_bytes().to_vec()),
            cfg: cfg.clone(),
            id_counter: Arc::new(AtomicU64::new(1)),
        });

        // spawn background expiration task only if timers are enabled
        if !cfg.disable_timers {
            let svc_cloned = Arc::clone(&svc);
            tokio::spawn(async move { svc_cloned.expiration_task().await });
        }

        svc
    }

    /// Acquire a lease for key (route string). If resource is busy the call
    /// will await until a lease is granted (FIFO order).
    pub async fn acquire(&self, key: String, ttl_secs: u32) -> Result<LeaseGrant, String> {
        if ttl_secs == 0 {
            return Err("invalid_ttl".to_string());
        }

        // fast path: try to create lease if none exists
        let mut guard = self.inner.lock().await;

        // check existing lease
        if let Some(entry) = guard.leases.get_mut(&key) {
            // if entry still active, enqueue
            if Instant::now() < entry.expiry {
                let (tx, rx) = oneshot::channel();
                entry.waiters.push_back(Pending {
                    requested_ttl: ttl_secs,
                    responder: tx,
                });
                drop(guard);
                // wait for grant
                match rx.await {
                    Ok(Ok(grant)) => return Ok(grant),
                    Ok(Err(e)) => return Err(e),
                    Err(_) => return Err("internal_error".to_string()),
                }
            }
        }

        // create and insert a new lease - use entry API to avoid clone
        let (entry, grant) = self.make_entry(&key, ttl_secs);
        let expiry = entry.expiry;
        use std::collections::hash_map::Entry;
        match guard.leases.entry(key.clone()) {
            Entry::Vacant(e) => {
                let key = e.key().clone();
                e.insert(entry);
                // Add to expiry heap
                guard.expiry_heap.push(ExpiryItem { key, expiry });
            }
            Entry::Occupied(mut e) => {
                // Race condition: entry was created between get_mut and here
                // This is rare, but handle by replacing
                let key = e.key().clone();
                e.insert(entry);
                // Add to expiry heap (old entry will be filtered out during expiration)
                guard.expiry_heap.push(ExpiryItem { key, expiry });
            }
        }
        // notify expiration task of new earliest expiry
        self.notify.notify_one();
        drop(guard);

        Ok(grant)
    }

    /// Extend an existing lease by add_secs. Returns remaining seconds.
    pub async fn extend(
        &self,
        key: String,
        id: &str,
        token: &str,
        add_secs: u32,
    ) -> Result<u32, String> {
        if add_secs == 0 {
            return Err("invalid_ttl".to_string());
        }
        let mut guard = self.inner.lock().await;
        let entry = guard
            .leases
            .get_mut(&key)
            .ok_or_else(|| "lease_not_found".to_string())?;
        if entry.id != id {
            return Err("invalid_token".to_string());
        }
        if entry.token != token {
            return Err("invalid_token".to_string());
        }
        if Instant::now() >= entry.expiry {
            return Err("lease_expired".to_string());
        }

        // extend by add_secs from current expiry (preserve remaining time)
        entry.expiry += Duration::from_secs(add_secs as u64);
        entry.ttl_secs = entry.ttl_secs.saturating_add(add_secs);
        let new_expiry = entry.expiry;
        
        // Add new expiry to heap (old entry will be filtered out)
        guard.expiry_heap.push(ExpiryItem {
            key: key.clone(),
            expiry: new_expiry,
        });
        
        // notify expiration task since deadline changed
        self.notify.notify_one();
        let remaining = new_expiry
            .saturating_duration_since(Instant::now())
            .as_secs() as u32;
        Ok(remaining)
    }

    /// Release a lease if id+token match. If waiters exist, grant next waiter.
    pub async fn release(&self, key: String, id: &str, token: &str) -> Result<(), String> {
        let mut guard = self.inner.lock().await;
        let mut entry = guard
            .leases
            .remove(&key)
            .ok_or_else(|| "lease_not_found".to_string())?;
        if entry.id != id || entry.token != token {
            // put it back unchanged
            guard.leases.insert(key.clone(), entry);
            return Err("invalid_token".to_string());
        }

        // if waiters exist, grant next
        if let Some(p) = entry.waiters.pop_front() {
            let responder = p.responder;
            let (mut new_entry, grant) =
                self.build_entry_and_grant_from_pending(&key, p.requested_ttl);
            // move remaining waiters (if any) from old entry into new one
            new_entry.waiters.append(&mut entry.waiters);
            let new_expiry = new_entry.expiry;
            guard.leases.insert(key.clone(), new_entry);
            
            // Add new lease to expiry heap
            guard.expiry_heap.push(ExpiryItem {
                key: key.clone(),
                expiry: new_expiry,
            });

            // notify expiration task
            self.notify.notify_one();

            // respond to waiter (best-effort; ignore send error)
            let _ = responder.send(Ok(grant));
        }

        Ok(())
    }

    /// Peek current lease if any
    pub async fn peek(&self, key: &str) -> Option<(String, Option<Vec<u8>>)> {
        let guard = self.inner.lock().await;
        if let Some(e) = guard.leases.get(key) {
            if Instant::now() < e.expiry {
                return Some((e.id.clone(), e.body.clone()));
            }
        }
        None
    }

    fn compute_token(&self, key: &str, id: &str, expiry: Instant) -> String {
        if matches!(self.cfg.token_mode, TokenMode::Fast) {
            // fast, allocation-light, deterministic (not cryptographic)
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut h);
            id.hash(&mut h);
            Self::expiry_unix_from_instant(expiry).hash(&mut h);
            return format!("{:016x}", h.finish());
        }

        // token = base64(hmac(secret, key|id|expiry_unix))
        // Optimized: write bytes directly to HMAC instead of format! allocation
        let expiry_unix = Self::expiry_unix_from_instant(expiry);

        let mut mac =
            HmacSha256::new_from_slice(&self.secret).expect("HMAC can take key of any size");
        
        // Write components directly without allocation
        mac.update(key.as_bytes());
        mac.update(b"|");
        mac.update(id.as_bytes());
        mac.update(b"|");
        
        // Convert expiry to bytes without allocation
        let mut buf = [0u8; 20]; // u64 max is 20 digits
        let mut temp = expiry_unix;
        let mut len = 0;
        if temp == 0 {
            buf[0] = b'0';
            len = 1;
        } else {
            while temp > 0 {
                buf[len] = b'0' + (temp % 10) as u8;
                temp /= 10;
                len += 1;
            }
            buf[0..len].reverse();
        }
        mac.update(&buf[0..len]);
        
        let result = mac.finalize().into_bytes();
        general_purpose::STANDARD.encode(result)
    }

    // Helper: compute expiry unix seconds from Instant
    fn expiry_unix_from_instant(expiry: Instant) -> u64 {
        (std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            + (expiry.saturating_duration_since(Instant::now())))
        .as_secs()
    }

    // Helper: create a new LeaseEntry and corresponding LeaseGrant
    fn make_entry(&self, key: &str, ttl_secs: u32) -> (LeaseEntry, LeaseGrant) {
        let id = if self.cfg.fast_ids {
            // Fast mode: sequential counter (no crypto, no allocation)
            self.id_counter.fetch_add(1, AtomicOrdering::Relaxed).to_string()
        } else {
            // Production mode: secure UUID
            Uuid::new_v4().to_string()
        };
        let expiry = Instant::now() + Duration::from_secs(ttl_secs as u64);
        let token = self.compute_token(key, &id, expiry);

        let entry = LeaseEntry {
            id: id.clone(),
            token: token.clone(),
            expiry,
            ttl_secs,
            body: None,
            waiters: VecDeque::new(),
        };

        let grant = LeaseGrant {
            id,
            body: None,
            token,
            ttl_secs,
        };
        (entry, grant)
    }

    // Helper: build a new entry and grant for a pending waiter
    fn build_entry_and_grant_from_pending(
        &self,
        key: &str,
        requested_ttl: u32,
    ) -> (LeaseEntry, LeaseGrant) {
        let new_id = if self.cfg.fast_ids {
            self.id_counter.fetch_add(1, AtomicOrdering::Relaxed).to_string()
        } else {
            Uuid::new_v4().to_string()
        };
        let expiry = Instant::now() + Duration::from_secs(requested_ttl as u64);
        let new_token = self.compute_token(key, &new_id, expiry);
        let grant = LeaseGrant {
            id: new_id.clone(),
            body: None,
            token: new_token.clone(),
            ttl_secs: requested_ttl,
        };

        let new_entry = LeaseEntry {
            id: new_id,
            token: new_token,
            expiry,
            ttl_secs: requested_ttl,
            body: None,
            waiters: VecDeque::new(),
        };

        (new_entry, grant)
    }

    // Helper: find next deadline using priority queue - O(1) instead of O(n)
    async fn find_next_deadline(&self) -> Option<(String, Instant)> {
        let mut guard = self.inner.lock().await;
        
        // Pop stale entries until we find a valid one or heap is empty
        while let Some(item) = guard.expiry_heap.peek() {
            let key = &item.key;
            let expiry = item.expiry;
            
            // Check if this entry matches current lease state
            if let Some(entry) = guard.leases.get(key) {
                if entry.expiry == expiry {
                    // Valid entry found
                    return Some((key.clone(), expiry));
                }
            }
            
            // Stale entry (lease was removed, extended, or replaced), remove it
            guard.expiry_heap.pop();
        }
        
        None
    }

    /// Background expiration task: waits for next expiry and frees leases.
    async fn expiration_task(self: Arc<Self>) {
        loop {
            // determine next expiry using heap - O(1) peek
            let next_opt = self.find_next_deadline().await;
            let (_next_key, next_deadline) = match next_opt {
                Some((k, d)) => (Some(k), d),
                None => {
                    // no leases; wait until notified
                    self.notify.notified().await;
                    continue;
                }
            };

            let now = Instant::now();
            if next_deadline > now {
                tokio::select! {
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_deadline)) => {},
                    _ = self.notify.notified() => { continue; }
                }
            }

            // expire the lease(s) whose deadline has passed
            let expired_keys: Vec<String> = {
                let mut guard = self.inner.lock().await;
                let now = Instant::now();
                let mut expired = Vec::new();
                
                // Process all expired items from heap
                while let Some(item) = guard.expiry_heap.peek() {
                    if item.expiry > now {
                        break; // No more expired items
                    }
                    
                    let item = guard.expiry_heap.pop().unwrap();
                    let key = &item.key;
                    
                    // Verify this entry is still current
                    if let Some(entry) = guard.leases.get(key) {
                        if entry.expiry == item.expiry && entry.expiry <= now {
                            expired.push(key.clone());
                        }
                    }
                }
                
                // remove expired and collect waiters to be granted
                for k in &expired {
                    if let Some(mut e) = guard.leases.remove(k) {
                        if let Some(p) = e.waiters.pop_front() {
                            let responder = p.responder;
                            let (mut new_entry, grant) =
                                self.build_entry_and_grant_from_pending(k, p.requested_ttl);
                            new_entry.waiters.append(&mut e.waiters);
                            let new_expiry = new_entry.expiry;
                            guard.leases.insert(k.clone(), new_entry);
                            
                            // Add new lease to heap
                            guard.expiry_heap.push(ExpiryItem {
                                key: k.clone(),
                                expiry: new_expiry,
                            });

                            // notify the waiter (best-effort)
                            let _ = responder.send(Ok(grant));
                        }
                    }
                }
                expired
            };

            if !expired_keys.is_empty() {
                // loop again to process next expirations
                continue;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn should_acquire_lease_successfully() {
        // Arrange
        let svc = LeaseService::new();

        // Act
        let grant = svc.acquire("res1".to_string(), 2).await.unwrap();

        // Assert
        assert!(!grant.id.is_empty());
        assert!(!grant.token.is_empty());
        assert_eq!(grant.ttl_secs, 2);
        assert!(grant.body.is_none());
    }

    #[tokio::test]
    async fn should_peek_active_lease() {
        // Arrange
        let svc = LeaseService::new();
        let grant = svc.acquire("res1".to_string(), 2).await.unwrap();

        // Act
        let peek = svc.peek("res1").await;

        // Assert
        assert!(peek.is_some());
        let (id, _body) = peek.unwrap();
        assert_eq!(id, grant.id);
    }

    #[tokio::test]
    async fn should_peek_returns_none_when_no_lease() {
        // Arrange
        let svc = LeaseService::new();

        // Act
        let peek = svc.peek("no-such").await;

        // Assert
        assert!(peek.is_none());
    }

    #[tokio::test]
    async fn should_reject_acquire_with_zero_ttl() {
        // Arrange
        let svc = LeaseService::new();

        // Act
        let result = svc.acquire("res".to_string(), 0).await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_ttl");
    }

    #[tokio::test]
    async fn should_extend_active_lease() {
        // Arrange
        let svc = LeaseService::new();
        let grant = svc.acquire("res2".to_string(), 2).await.unwrap();

        // Act
        let added = 10u32;
        let remaining = svc
            .extend("res2".to_string(), &grant.id, &grant.token, added)
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
        let svc = LeaseService::new();
        let grant = svc.acquire("res".to_string(), 5).await.unwrap();

        // Act
        let result = svc
            .extend("res".to_string(), &grant.id, &grant.token, 0)
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_ttl");
    }

    #[tokio::test]
    async fn should_reject_extend_with_wrong_id() {
        // Arrange
        let svc = LeaseService::new();
        let grant = svc.acquire("res".to_string(), 5).await.unwrap();

        // Act
        let result = svc
            .extend("res".to_string(), "wrong-id", &grant.token, 5)
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");
    }

    #[tokio::test]
    async fn should_reject_extend_with_wrong_token() {
        // Arrange
        let svc = LeaseService::new();
        let grant = svc.acquire("res".to_string(), 5).await.unwrap();

        // Act
        let result = svc
            .extend("res".to_string(), &grant.id, "wrong-token", 5)
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");
    }

    #[tokio::test]
    async fn should_reject_extend_for_nonexistent_lease() {
        // Arrange
        let svc = LeaseService::new();

        // Act
        let result = svc
            .extend("no-lease".to_string(), "fake-id", "fake-token", 5)
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_not_found");
    }

    #[tokio::test]
    async fn should_reject_extend_for_expired_lease() {
        // Arrange
        let svc = LeaseService::new();
        let grant = svc.acquire("res".to_string(), 1).await.unwrap();

        // wait for lease to expire (expiration task may clean it up)
        sleep(Duration::from_secs(2)).await;

        // Act
        let result = svc
            .extend("res".to_string(), &grant.id, &grant.token, 5)
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
        let svc = LeaseService::new();
        let grant = svc.acquire("res".to_string(), 5).await.unwrap();

        // Act
        let result = svc
            .release("res".to_string(), &grant.id, &grant.token)
            .await;

        // Assert
        assert!(result.is_ok());

        // verify lease is gone
        let peek = svc.peek("res").await;
        assert!(peek.is_none());
    }

    #[tokio::test]
    async fn should_reject_release_with_wrong_id() {
        // Arrange
        let svc = LeaseService::new();
        let grant = svc.acquire("res".to_string(), 5).await.unwrap();

        // Act
        let result = svc
            .release("res".to_string(), "wrong-id", &grant.token)
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");

        // lease should still exist
        let peek = svc.peek("res").await;
        assert!(peek.is_some());
    }

    #[tokio::test]
    async fn should_reject_release_with_wrong_token() {
        // Arrange
        let svc = LeaseService::new();
        let grant = svc.acquire("res".to_string(), 5).await.unwrap();

        // Act
        let result = svc
            .release("res".to_string(), &grant.id, "wrong-token")
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_token");

        // lease should still exist
        let peek = svc.peek("res").await;
        assert!(peek.is_some());
    }

    #[tokio::test]
    async fn should_reject_release_for_nonexistent_lease() {
        // Arrange
        let svc = LeaseService::new();

        // Act
        let result = svc
            .release("no-lease".to_string(), "fake-id", "fake-token")
            .await;

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "lease_not_found");
    }

    #[tokio::test]
    async fn should_enqueue_waiter_when_lease_busy() {
        // Arrange
        let svc = LeaseService::new();
        let _holder = svc.acquire("res".to_string(), 10).await.unwrap();

        // spawn a second acquire that should wait
        let svc_clone = svc.clone();
        let waiter = tokio::spawn(async move { svc_clone.acquire("res".to_string(), 5).await });

        // give waiter time to enqueue
        sleep(Duration::from_millis(50)).await;

        // Act - waiter should still be pending
        // We can't directly assert it's waiting, but we can verify it hasn't completed yet
        assert!(!waiter.is_finished());
    }

    #[tokio::test]
    async fn should_grant_lease_to_waiter_on_release() {
        // Arrange
        let svc = LeaseService::new();
        let holder = svc.acquire("res3".to_string(), 5).await.unwrap();

        // spawn a waiter that will block until the lease is released
        let svc_clone = svc.clone();
        let waiter = tokio::spawn(async move { svc_clone.acquire("res3".to_string(), 3).await });

        // give the waiter a moment to enqueue
        sleep(Duration::from_millis(50)).await;

        // Act
        svc.release("res3".to_string(), &holder.id, &holder.token)
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
        let svc = LeaseService::new();
        let holder = svc.acquire("res".to_string(), 5).await.unwrap();

        // spawn two waiters
        let svc1 = svc.clone();
        let waiter1 = tokio::spawn(async move { svc1.acquire("res".to_string(), 2).await });

        sleep(Duration::from_millis(10)).await;

        let svc2 = svc.clone();
        let waiter2 = tokio::spawn(async move { svc2.acquire("res".to_string(), 3).await });

        sleep(Duration::from_millis(50)).await;

        // Act - release should grant to first waiter
        svc.release("res".to_string(), &holder.id, &holder.token)
            .await
            .unwrap();

        // Assert - first waiter gets the lease
        let grant1 = waiter1.await.unwrap().unwrap();
        assert_eq!(grant1.ttl_secs, 2);

        // second waiter should still be waiting
        sleep(Duration::from_millis(50)).await;
        assert!(
            !waiter2.is_finished(),
            "second waiter should still be blocked"
        );
    }

    #[tokio::test]
    async fn should_expire_lease_and_grant_to_waiter() {
        // Arrange
        let svc = LeaseService::new();
        let _holder = svc.acquire("res".to_string(), 1).await.unwrap();

        // spawn a waiter
        let svc_clone = svc.clone();
        let waiter = tokio::spawn(async move { svc_clone.acquire("res".to_string(), 2).await });

        // Act - wait for lease to expire
        sleep(Duration::from_secs(2)).await;

        // Assert - waiter should be granted
        let grant = waiter.await.unwrap().unwrap();
        assert!(!grant.id.is_empty());
        assert_eq!(grant.ttl_secs, 2);
    }

    #[tokio::test]
    async fn should_handle_multiple_keys_independently() {
        // Arrange
        let svc = LeaseService::new();

        // Act
        let grant1 = svc.acquire("key1".to_string(), 5).await.unwrap();
        let grant2 = svc.acquire("key2".to_string(), 5).await.unwrap();

        // Assert
        assert_ne!(grant1.id, grant2.id);

        // verify both exist
        let peek1 = svc.peek("key1").await;
        let peek2 = svc.peek("key2").await;
        assert!(peek1.is_some());
        assert!(peek2.is_some());

        // release one shouldn't affect the other
        svc.release("key1".to_string(), &grant1.id, &grant1.token)
            .await
            .unwrap();
        assert!(svc.peek("key1").await.is_none());
        assert!(svc.peek("key2").await.is_some());
    }

    #[tokio::test]
    async fn should_allow_reacquire_after_expiration() {
        // Arrange
        let svc = LeaseService::new();
        let grant1 = svc.acquire("res".to_string(), 1).await.unwrap();

        // wait for expiration
        sleep(Duration::from_secs(2)).await;

        // Act - acquire again
        let grant2 = svc.acquire("res".to_string(), 5).await.unwrap();

        // Assert
        assert_ne!(grant1.id, grant2.id, "new lease should have different ID");
        assert_ne!(
            grant1.token, grant2.token,
            "new lease should have different token"
        );
    }
}
