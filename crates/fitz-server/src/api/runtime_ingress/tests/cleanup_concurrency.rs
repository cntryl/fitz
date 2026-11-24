use super::*;
use crate::runtime::{DeliveryError, Envelope, MailboxSink};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct BlockingCleanupSink {
    entered: crossbeam_channel::Sender<u64>,
    release: crossbeam_channel::Receiver<()>,
    blocked_sessions: Mutex<HashSet<u64>>,
}

impl MailboxSink for BlockingCleanupSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let cleanup = envelope
            .payload::<crate::runtime::SessionCleanup>()
            .expect("cleanup payload");
        if self
            .blocked_sessions
            .lock()
            .expect("lock blocked sessions")
            .insert(cleanup.session_id)
        {
            self.entered
                .send(cleanup.session_id)
                .expect("record entered cleanup");
            self.release.recv().expect("release cleanup");
        }
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[tokio::test]
async fn should_bound_concurrent_independent_session_cleanups() {
    // Arrange
    const SESSION_COUNT: usize = 40;
    const CLEANUP_CONCURRENCY: usize = 32;
    let router = Arc::new(crate::runtime::Router::new());
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(SESSION_COUNT);
    let (release_tx, release_rx) = crossbeam_channel::bounded(SESSION_COUNT);
    let sink = Arc::new(BlockingCleanupSink {
        entered: entered_tx,
        release: release_rx,
        blocked_sessions: Mutex::new(HashSet::new()),
    });
    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        router.register_domain_pattern(domain.as_str(), sink.clone());
    }
    let ingress = Arc::new(make_cleanup_ingress(router, AdminReadModel::new()));
    for session_id in 1..=SESSION_COUNT {
        ingress
            .on_open(make_session_info(
                u64::try_from(session_id).expect("session id"),
                TransportKind::Tcp,
            ))
            .await
            .expect("open session");
    }

    // Act
    let mut closes = Vec::with_capacity(SESSION_COUNT);
    for session_id in 1..=SESSION_COUNT {
        let ingress = ingress.clone();
        closes.push(tokio::spawn(async move {
            ingress
                .on_close(
                    u64::try_from(session_id).expect("session id"),
                    CloseReason::ClientClose,
                )
                .await;
        }));
    }
    let overflow = tokio::task::spawn_blocking(move || {
        for _ in 0..CLEANUP_CONCURRENCY {
            entered_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("bounded cleanup should start");
        }
        let overflow = entered_rx.recv_timeout(Duration::from_millis(100));
        let mut entered = CLEANUP_CONCURRENCY + usize::from(overflow.is_ok());
        for _ in 0..entered {
            release_tx.send(()).expect("release initial cleanup");
        }
        while entered < SESSION_COUNT {
            entered_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("remaining cleanup should start");
            entered += 1;
            release_tx.send(()).expect("release remaining cleanup");
        }
        overflow
    })
    .await
    .expect("join cleanup observation");
    for close in closes {
        close.await.expect("join close");
    }

    // Assert
    assert!(overflow.is_err(), "more than 32 cleanups ran concurrently");
    assert_eq!(ingress.session_count(), 0);
}

#[tokio::test]
async fn should_keep_running_cleanup_jobs_bounded_after_close_cancellation() {
    // Arrange
    const CLEANUP_CONCURRENCY: usize = 32;
    const SESSION_COUNT: usize = CLEANUP_CONCURRENCY + 1;
    let router = Arc::new(crate::runtime::Router::new());
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(SESSION_COUNT);
    let (release_tx, release_rx) = crossbeam_channel::bounded(SESSION_COUNT);
    let sink = Arc::new(BlockingCleanupSink {
        entered: entered_tx,
        release: release_rx,
        blocked_sessions: Mutex::new(HashSet::new()),
    });
    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        router.register_domain_pattern(domain.as_str(), sink.clone());
    }
    let ingress = Arc::new(make_cleanup_ingress(router, AdminReadModel::new()));
    for session_id in 1..=SESSION_COUNT {
        ingress
            .on_open(make_session_info(
                u64::try_from(session_id).expect("session id"),
                TransportKind::Tcp,
            ))
            .await
            .expect("open session");
    }

    // Act
    let mut interrupted_closes = Vec::with_capacity(CLEANUP_CONCURRENCY);
    for session_id in 1..=CLEANUP_CONCURRENCY {
        let ingress = ingress.clone();
        interrupted_closes.push(tokio::spawn(async move {
            ingress
                .on_close(
                    u64::try_from(session_id).expect("session id"),
                    CloseReason::ClientClose,
                )
                .await;
        }));
    }
    let initial_entries = entered_rx.clone();
    tokio::task::spawn_blocking(move || {
        for _ in 0..CLEANUP_CONCURRENCY {
            initial_entries
                .recv_timeout(Duration::from_secs(1))
                .expect("cleanup should enter blocking sink");
        }
    })
    .await
    .expect("join initial cleanup observation");
    for close in interrupted_closes {
        close.abort();
        assert!(close
            .await
            .expect_err("close should be canceled")
            .is_cancelled());
    }
    let remaining_ingress = ingress.clone();
    let remaining_close = tokio::spawn(async move {
        remaining_ingress
            .on_close(
                u64::try_from(SESSION_COUNT).expect("session id"),
                CloseReason::ClientClose,
            )
            .await;
    });
    let overflow = tokio::task::spawn_blocking(move || {
        let overflow = entered_rx.recv_timeout(Duration::from_millis(100));
        for _ in 0..CLEANUP_CONCURRENCY + usize::from(overflow.is_ok()) {
            release_tx.send(()).expect("release blocked cleanup");
        }
        if overflow.is_err() {
            entered_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("remaining cleanup should start after release");
            release_tx.send(()).expect("release remaining cleanup");
        }
        overflow
    })
    .await
    .expect("join canceled cleanup observation");
    remaining_close.await.expect("join remaining close");
    ingress.close_all_sessions(CloseReason::ClientClose).await;

    // Assert
    assert!(
        overflow.is_err(),
        "canceling close awaiters must not admit more than 32 running cleanup jobs"
    );
}
