use std::collections::HashMap;
use tokio::sync::mpsc::error::TrySendError;

use crate::core::domain::SubSender;

/// Subscription entry maintained by the Router
#[derive(Debug, Clone)]
pub struct SubEntry {
    pub id: u64,
    pub route: String,
    pub channel_id: u32,
    pub sender: SubSender,
}

/// Router holds an in-memory registry of subscriptions and provides
/// route matching utilities for notice dispatch.
#[derive(Debug, Default)]
pub struct Router {
    next_id: u64,
    subs: HashMap<u64, SubEntry>,
    /// Round-robin index per route pattern for RPC load balancing
    rpc_round_robin: HashMap<String, usize>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            subs: HashMap::new(),
            rpc_round_robin: HashMap::new(),
        }
    }

    /// Register a subscription to a route pattern on a specific channel.
    /// Returns a unique subscription id.
    pub fn subscribe(&mut self, route: String, channel_id: u32, sender: SubSender) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        self.subs.insert(
            id,
            SubEntry {
                id,
                route,
                channel_id,
                sender,
            },
        );
        id
    }

    /// Remove a subscription by id. Returns true if something was removed.
    pub fn unsubscribe(&mut self, id: u64) -> bool {
        self.subs.remove(&id).is_some()
    }

    /// Remove all subscriptions for a given channel (e.g., on disconnect)
    pub fn cleanup_channel(&mut self, channel_id: u32) {
        self.subs.retain(|_, s| s.channel_id != channel_id);
    }

    /// Dispatch a notification to all matching subscribers. Uses try_send to avoid blocking.
    /// For RPC routes (rpc://*), uses round-robin to deliver to only one subscriber.
    /// For other routes, broadcasts to all matching subscribers.
    /// Returns (delivered_count, removed_dead_subs)
    pub fn dispatch(
        &mut self,
        route: &str,
        msg_id: Option<&str>,
        body: &[u8],
        reply_to: Option<&str>,
        seq: Option<u32>,
        end: bool,
    ) -> (usize, Vec<u64>) {
        let is_rpc = route.starts_with("rpc://");

        if is_rpc {
            // RPC: Round-robin delivery to single subscriber
            self.dispatch_rpc_round_robin(route, msg_id, body, reply_to, seq, end)
        } else {
            // Notice/Queue/Stream: Broadcast to all subscribers
            self.dispatch_broadcast(route, msg_id, body, reply_to, seq, end)
        }
    }

    /// Broadcast to all matching subscribers (notice, queue, stream, etc.)
    fn dispatch_broadcast(
        &mut self,
        route: &str,
        msg_id: Option<&str>,
        body: &[u8],
        reply_to: Option<&str>,
        seq: Option<u32>,
        end: bool,
    ) -> (usize, Vec<u64>) {
        let mut delivered = 0usize;
        let mut to_remove = Vec::new();
        for (sub_id, sub) in self.subs.iter() {
            if route_matches(&sub.route, route) {
                if let Err(e) = sub.sender.try_send((
                    route.to_string(),
                    msg_id.map(|s| s.to_string()),
                    body.to_vec(),
                    reply_to.map(|s| s.to_string()),
                    seq,
                    end,
                )) {
                    match e {
                        TrySendError::Closed(_) => to_remove.push(*sub_id),
                        TrySendError::Full(_) => { /* drop on backpressure (best-effort) */ }
                    }
                } else {
                    delivered += 1;
                }
            }
        }
        // prune closed subscriptions
        if !to_remove.is_empty() {
            for id in &to_remove {
                self.subs.remove(id);
            }
        }
        (delivered, to_remove)
    }

    /// Round-robin delivery to single subscriber for RPC routes
    fn dispatch_rpc_round_robin(
        &mut self,
        route: &str,
        msg_id: Option<&str>,
        body: &[u8],
        reply_to: Option<&str>,
        seq: Option<u32>,
        end: bool,
    ) -> (usize, Vec<u64>) {
        // Collect matching subscribers
        let mut matching_subs: Vec<(&u64, &SubEntry)> = self
            .subs
            .iter()
            .filter(|(_, sub)| route_matches(&sub.route, route))
            .collect();

        if matching_subs.is_empty() {
            return (0, vec![]);
        }

        // Sort for deterministic round-robin
        matching_subs.sort_by_key(|(id, _)| *id);

        // Get current round-robin index for this route pattern
        let current_idx = self.rpc_round_robin.entry(route.to_string()).or_insert(0);
        let selected_idx = *current_idx % matching_subs.len();

        // Update round-robin index for next call
        *current_idx = (*current_idx + 1) % matching_subs.len();

        // Deliver to selected subscriber
        let (sub_id, sub) = matching_subs[selected_idx];
        let mut to_remove = Vec::new();
        let delivered = if let Err(e) = sub.sender.try_send((
            route.to_string(),
            msg_id.map(|s| s.to_string()),
            body.to_vec(),
            reply_to.map(|s| s.to_string()),
            seq,
            end,
        )) {
            match e {
                TrySendError::Closed(_) => {
                    to_remove.push(*sub_id);
                    0
                }
                TrySendError::Full(_) => 0, // Drop on backpressure
            }
        } else {
            1
        };

        // Prune closed subscription
        if !to_remove.is_empty() {
            for id in &to_remove {
                self.subs.remove(id);
            }
        }

        (delivered, to_remove)
    }

    /// Return a snapshot count of current subscriptions
    pub fn len(&self) -> usize {
        self.subs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.subs.is_empty()
    }
}

/// Simple route pattern matching for notices
/// - Exact match
/// - Trailing '*' wildcard as prefix match (e.g., "a/b/*")
/// - Hierarchical prefix match (e.g., "a/b" matches "a/b/c")
pub fn route_matches(pattern: &str, route: &str) -> bool {
    if pattern == route {
        return true;
    }

    // global wildcard
    if pattern == "*" {
        return true;
    }

    // trailing '*' treated as prefix
    if let Some(stripped) = pattern.strip_suffix('*') {
        return route.starts_with(stripped);
    }

    // hierarchical prefix: "a/b" matches "a/b" or "a/b/..."
    if let Some(rem) = route.strip_prefix(pattern) {
        return rem.is_empty() || rem.starts_with('/');
    }

    false
}

// Inline unit tests (kept in-file per new guideline)

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn should_match_routes_exact_and_global() {
        // Arrange
        // patterns and routes are literals for this pure function

        // Act
        let a = route_matches("a/b", "a/b");
        let b = route_matches("*", "anything");
        let c = route_matches("a/b", "a/bb");

        // Assert
        assert!(a);
        assert!(b);
        assert!(!c);
    }

    #[test]
    fn should_match_trailing_star_prefix() {
        // Arrange

        // Act
        let a = route_matches("a/b/*", "a/b/c");
        let b = route_matches("a/*", "a/b/c");
        let c = route_matches("a/b/*", "a/x/c");

        // Assert
        assert!(a);
        assert!(b);
        assert!(!c);
    }

    #[test]
    fn should_match_hierarchical_prefix() {
        // Arrange

        // Act
        let a = route_matches("a/b", "a/b/c");
        let b = route_matches("a/b", "a/b");
        let c = route_matches("a/b", "a/ba/c");

        // Assert
        assert!(a);
        assert!(b);
        assert!(!c);
    }

    #[test]
    fn should_subscribe_unsubscribe_len_is_empty() {
        // Arrange
        let mut r = Router::new();
        let (tx, _rx) = mpsc::channel::<(
            String,
            Option<String>,
            Vec<u8>,
            Option<String>,
            Option<u32>,
            bool,
        )>(4);

        // Act
        let id = r.subscribe("foo".to_string(), 1, tx);

        // Assert
        assert_eq!(r.len(), 1);
        assert!(!r.is_empty());
        assert!(r.unsubscribe(id));
        assert_eq!(r.len(), 0);
        assert!(r.is_empty());
    }

    #[test]
    fn should_cleanup_channel() {
        // Arrange
        let mut r = Router::new();
        let (tx1, _rx1) = mpsc::channel(4);
        let (tx2, _rx2) = mpsc::channel(4);
        let id1 = r.subscribe("a".to_string(), 10, tx1);
        let _id2 = r.subscribe("b".to_string(), 20, tx2);

        // Act
        r.cleanup_channel(10);

        // Assert
        assert_eq!(r.len(), 1);
        // ensure specific id removed
        assert!(!r.unsubscribe(id1));
    }

    #[test]
    fn should_dispatch_success_and_receive() {
        // Arrange
        let mut r = Router::new();
        let (tx, mut rx) = mpsc::channel(4);
        let _id = r.subscribe("route/a".to_string(), 1, tx);

        // Act
        let (delivered, removed) = r.dispatch("route/a", Some("mid"), b"hi", None, Some(1), false);

        // Assert
        assert_eq!(delivered, 1);
        assert!(removed.is_empty());

        // try_recv should yield the message we dispatched
        let got = rx.try_recv();
        assert!(got.is_ok());
        let (rout, mid, body, _reply, seq, end) = got.unwrap();
        assert_eq!(rout, "route/a");
        assert_eq!(mid.unwrap(), "mid");
        assert_eq!(body, b"hi".to_vec());
        assert_eq!(seq, Some(1));
        assert!(!end);
    }

    #[test]
    fn should_not_remove_on_full_backpressure() {
        // Arrange
        let mut r = Router::new();
        // small capacity so we can force Full
        let (tx, mut rx) = mpsc::channel(1);
        // pre-fill the channel so next try_send will return Full
        tx.try_send(("prefill".to_string(), None, vec![0u8], None, None, false))
            .expect("prefill send");

        // Act
        let _id = r.subscribe("x".to_string(), 1, tx);
        let (delivered, removed) = r.dispatch("x", None, b"v", None, None, false);

        // Assert
        // Full should not count as delivered and should not remove
        assert_eq!(delivered, 0);
        assert!(removed.is_empty());
        assert_eq!(r.len(), 1);

        // drain the prefill so we don't leak
        let _ = rx.try_recv();
        // now a subsequent dispatch should succeed
        let (delivered2, _) = r.dispatch("x", None, b"v2", None, None, false);
        assert_eq!(delivered2, 1);
    }

    #[test]
    fn should_remove_closed_sub_on_dispatch() {
        // Arrange
        let mut r = Router::new();
        let (tx, rx) = mpsc::channel(4);
        let id = r.subscribe("rm".to_string(), 2, tx);

        // Act
        // drop receiver to close
        drop(rx);
        let (delivered, removed) = r.dispatch("rm", None, b"x", None, None, false);

        // Assert
        assert_eq!(delivered, 0);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0], id);
        // router should have pruned the closed sub
        assert_eq!(r.len(), 0);
    }
}
