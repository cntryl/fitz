//! Lease domain service - ephemeral resource locking

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::sync::{oneshot, Mutex, Notify};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

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

struct Inner {
    leases: HashMap<String, LeaseEntry>,
}

/// In-memory lease service. All data is kept in-process only.
#[derive(Clone)]
pub struct LeaseService {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Notify>,
    secret: Arc<Vec<u8>>,
}

impl LeaseService {
    /// Create a new LeaseService and spawn the expiration task.
    pub fn new() -> Arc<Self> {
        let svc = Arc::new(Self {
            inner: Arc::new(Mutex::new(Inner {
                leases: HashMap::new(),
            })),
            notify: Arc::new(Notify::new()),
            // generate a random secret for HMAC (in-memory only)
            secret: Arc::new(Uuid::new_v4().as_bytes().to_vec()),
        });

        // spawn background expiration task
        {
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

        // create and insert a new lease
        let (entry, grant) = self.make_entry(&key, ttl_secs);
        guard.leases.insert(key.clone(), entry);
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
        // notify expiration task since deadline changed
        self.notify.notify_one();
        let remaining = entry
            .expiry
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
            guard.leases.insert(key.clone(), new_entry);

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
        // token = base64(hmac(secret, key|id|expiry_unix))
        let expiry_unix = Self::expiry_unix_from_instant(expiry);

        let mut mac =
            HmacSha256::new_from_slice(&self.secret).expect("HMAC can take key of any size");
        let ctx = format!("{}|{}|{}|{}", key, id, expiry_unix, Uuid::new_v4());
        mac.update(ctx.as_bytes());
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
        let id = Uuid::new_v4().to_string();
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
        let new_id = Uuid::new_v4().to_string();
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

    // Helper: find next deadline (key, deadline) if any
    async fn find_next_deadline(&self) -> Option<(String, Instant)> {
        let guard = self.inner.lock().await;
        let mut min_key: Option<String> = None;
        let mut min_deadline = Instant::now() + Duration::from_secs(24 * 3600 * 365);
        for (k, v) in guard.leases.iter() {
            if v.expiry < min_deadline {
                min_deadline = v.expiry;
                min_key = Some(k.clone());
            }
        }
        min_key.map(|k| (k, min_deadline))
    }

    /// Background expiration task: waits for next expiry and frees leases.
    async fn expiration_task(self: Arc<Self>) {
        loop {
            // determine next expiry using helper
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
                    _ = tokio::time::sleep_until(next_deadline.into()) => {},
                    _ = self.notify.notified() => { continue; }
                }
            }

            // expire the lease(s) whose deadline has passed
            let expired_keys: Vec<String> = {
                let mut guard = self.inner.lock().await;
                let now = Instant::now();
                let mut expired = Vec::new();
                for (k, v) in guard.leases.iter() {
                    if v.expiry <= now {
                        expired.push(k.clone());
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
                            guard.leases.insert(k.clone(), new_entry);

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
