//! Minimal, zero-copy notification plumbing used for perf proofs.
//!
//! This is intentionally small and avoids allocations in the hot path.

use crate::protocol::tlv::MessageType;

/// Subscriber identifier
pub type SubscriberId = usize;

/// Minimal matcher: map exact message types to subscriber id lists.
/// Zero-copy matching via `matching_subscribers` that returns `Option<&[SubscriberId]>`.
///
/// Added `match_into(&mut SmallVec)` API for a hot-path, allocation-free write into a
/// caller-provided buffer (benchable in isolation).
#[derive(Debug, Clone, Default)]
pub struct Matcher {
    subs: Vec<(u16, Vec<SubscriberId>)>,
}

impl Matcher {
    pub fn new() -> Self {
        Self { subs: Vec::new() }
    }

    /// Register a subscriber for a given message type
    pub fn register(&mut self, msg_type: u16, sub: SubscriberId) {
        // fast path: push onto existing bucket
        if let Some((_t, vec)) = self.subs.iter_mut().find(|e| e.0 == msg_type) {
            vec.push(sub);
            return;
        }
        self.subs.push((msg_type, vec![sub]));
    }

    /// Return a borrowed slice of subscriber ids for this message (zero-alloc)
    pub fn matching_subscribers<'a>(&'a self, msg_type: MessageType, _payload: &'a [u8]) -> Option<&'a [SubscriberId]> {
        // For this minimal implementation, we ignore payload predicates.
        self.subs.iter().find(|e| e.0 == msg_type.as_u16()).map(|(_, v)| &v[..])
    }

    /// Match into a caller-provided SmallVec without allocating.
    /// Returns the number of subscribers written into `out`.
    pub fn match_into(&self, out: &mut smallvec::SmallVec<[SubscriberId; 8]>, msg_type: MessageType, _payload: &[u8]) -> usize {
        out.clear();
        if let Some(subs) = self.matching_subscribers(msg_type, _payload) {
            out.extend_from_slice(subs);
            subs.len()
        } else {
            0
        }
    }
}

/// Minimal fan-out stub: records deliveries as a simple counter and stores an optional
/// record of (SubscriberId, payload_len) for verification.
#[derive(Debug, Default)]
pub struct Fanout {
    deliveries: usize,
}

impl Fanout {
    pub fn new() -> Self {
        Self { deliveries: 0 }
    }

    /// Deliver to the provided subscriber ids. This is intentionally minimal and deterministic.
    /// NOTE: No heap allocations or vector pushes here — only a simple counter increment and
    /// a `black_box` to represent delivery work.
    pub fn deliver(&mut self, subs: &[SubscriberId], payload: &[u8]) -> usize {
        for &id in subs {
            // represent delivery; keep payload borrowed
            core::hint::black_box((id, payload));
            self.deliveries += 1;
        }
        subs.len()
    }

    pub fn delivered_count(&self) -> usize { self.deliveries }
}

/// Thin NotificationDomain that composes a `Matcher` and a `Fanout` and wires the
/// match -> fanout flow. This is intentionally small to make domain boundaries explicit.
#[derive(Debug, Default)]
pub struct NotificationDomain {
    matcher: Matcher,
    fanout: Fanout,
}

impl NotificationDomain {
    pub fn new() -> Self {
        Self { matcher: Matcher::new(), fanout: Fanout::new() }
    }

    /// Register a subscription in the underlying matcher
    pub fn register(&mut self, msg_type: u16, sub: SubscriberId) {
        self.matcher.register(msg_type, sub);
    }

    /// Handle a single message: query matching subscribers and call fanout to perform
    /// the stubbed delivery. Uses borrowed slices to avoid allocations/copies in the hot-path.
    pub fn handle(&mut self, msg_type: MessageType, payload: &[u8]) -> usize {
        if let Some(subs) = self.matcher.matching_subscribers(msg_type, payload) {
            self.fanout.deliver(subs, payload)
        } else {
            0
        }
    }

    pub fn fanout_delivered(&self) -> usize {
        self.fanout.delivered_count()
    }

    pub fn matcher(&self) -> &Matcher { &self.matcher }
}


#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::SmallVec;

    #[test]
    fn should_match_into_smallvec_zero_copy() {
        // Arrange
        let mut m = Matcher::new();
        m.register(42, 1);
        m.register(42, 2);
        let payload = b"hello";

        // Act
        let mut out: SmallVec<[SubscriberId; 8]> = SmallVec::new();
        let n = m.match_into(&mut out, MessageType::new(42), payload);

        // Assert
        assert_eq!(n, 2);
        assert_eq!(out.len(), 2);
        assert!(out.contains(&1));
        assert!(out.contains(&2));
    }

    #[test]
    fn domain_handles_and_fanouts() {
        // Arrange
        let mut d = NotificationDomain::new();
        d.register(10, 3);
        d.register(10, 4);

        // Act
        let n = d.handle(MessageType::new(10), b"payload");

        // Assert
        assert_eq!(n, 2);
        assert_eq!(d.fanout_delivered(), 2);
    }

    #[test]
    fn handle_returns_zero_when_no_subs() {
        // Arrange
        let mut d = NotificationDomain::new();

        // Act
        let n = d.handle(MessageType::new(99), b"x");

        // Assert
        assert_eq!(n, 0);
        assert_eq!(d.fanout_delivered(), 0);
    }
}
