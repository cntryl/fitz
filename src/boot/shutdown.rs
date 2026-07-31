use super::{BootResult, Runtime};
use std::sync::Weak;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShutdownSignal {
    CtrlC,
    Sigterm,
    DrainRequested,
    ActorFailure,
    StorageLeaseFailure,
}

impl ShutdownSignal {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::CtrlC => "ctrl_c",
            Self::Sigterm => "sigterm",
            Self::DrainRequested => "drain_requested",
            Self::ActorFailure => "actor_failure",
            Self::StorageLeaseFailure => "storage_lease_failure",
        }
    }

    pub(super) fn requires_drain(self) -> bool {
        matches!(self, Self::Sigterm | Self::DrainRequested)
    }

    pub(super) fn is_failure(self) -> bool {
        matches!(self, Self::ActorFailure | Self::StorageLeaseFailure)
    }

    fn priority(self) -> u8 {
        match self {
            Self::Sigterm | Self::DrainRequested => 1,
            Self::CtrlC => 2,
            Self::ActorFailure | Self::StorageLeaseFailure => 3,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ShutdownTrigger {
    sender: watch::Sender<Option<ShutdownSignal>>,
}

impl ShutdownTrigger {
    pub(crate) fn request(&self, signal: ShutdownSignal) -> bool {
        self.sender.send_if_modified(|current| {
            if current.is_some_and(|current| current.priority() >= signal.priority()) {
                return false;
            }
            *current = Some(signal);
            true
        })
    }
}

pub(super) struct ShutdownCoordinator {
    receiver: watch::Receiver<Option<ShutdownSignal>>,
    trigger: ShutdownTrigger,
    tasks: Vec<JoinHandle<()>>,
}

impl ShutdownCoordinator {
    pub(super) fn start() -> BootResult<Self> {
        let (sender, receiver) = watch::channel(None);
        let trigger = ShutdownTrigger { sender };
        let signal_task = start_os_signal_task(trigger.clone())?;
        Ok(Self {
            receiver,
            trigger,
            tasks: vec![signal_task],
        })
    }

    #[cfg(test)]
    pub(super) fn manual() -> (Self, ShutdownTrigger) {
        let (sender, receiver) = watch::channel(None);
        let trigger = ShutdownTrigger { sender };
        (
            Self {
                receiver,
                trigger: trigger.clone(),
                tasks: Vec::new(),
            },
            trigger,
        )
    }

    pub(super) fn monitor_runtime(&mut self, runtime: Runtime) {
        let trigger = self.trigger.clone();
        let mut requested_shutdown = self.receiver.clone();
        self.tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(25));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if requested_shutdown
                    .borrow()
                    .is_some_and(|signal| !signal.requires_drain())
                {
                    runtime.begin_drain();
                    return;
                }
                tokio::select! {
                    biased;
                    changed = requested_shutdown.changed() => {
                        if changed.is_err() {
                            runtime.begin_drain();
                            return;
                        }
                        let signal = *requested_shutdown.borrow();
                        if signal.is_some() {
                            runtime.begin_drain();
                        }
                        if signal.is_some_and(|signal| !signal.requires_drain()) {
                            return;
                        }
                    }
                    _ = interval.tick() => {
                        if runtime.has_fatal_domain_failure() {
                            trigger.request(ShutdownSignal::ActorFailure);
                            runtime.begin_drain();
                            return;
                        }
                        if runtime.has_permanently_failed_domain() {
                            tracing::error!(
                                "A domain actor failed; withdrawing readiness and beginning broker drain"
                            );
                            runtime.mark_fatal_domain_failure();
                            trigger.request(ShutdownSignal::ActorFailure);
                            runtime.begin_drain();
                            return;
                        }
                        if runtime.is_draining() {
                            trigger.request(ShutdownSignal::DrainRequested);
                        }
                        if runtime.is_shutting_down() {
                            return;
                        }
                    }
                }
            }
        }));
    }

    pub(super) fn monitor_storage(&mut self, runtime: Runtime, store: Weak<cntryl_midge::Engine>) {
        let trigger = self.trigger.clone();
        let mut requested_shutdown = self.receiver.clone();
        self.tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if requested_shutdown
                    .borrow()
                    .is_some_and(|signal| !signal.requires_drain())
                {
                    return;
                }
                tokio::select! {
                    biased;
                    changed = requested_shutdown.changed() => {
                        if changed.is_err()
                            || requested_shutdown
                                .borrow()
                                .is_some_and(|signal| !signal.requires_drain())
                        {
                            return;
                        }
                    }
                    _ = interval.tick() => {
                        if runtime.is_shutting_down() {
                            return;
                        }
                        let Some(store) = store.upgrade() else {
                            return;
                        };
                        let lease_is_healthy = store.is_primary_lease_healthy();
                        drop(store);
                        if !lease_is_healthy {
                            handle_storage_lease_failure(&runtime, &trigger);
                            return;
                        }
                    }
                }
            }
        }));
    }

    pub(super) fn receiver(&self) -> watch::Receiver<Option<ShutdownSignal>> {
        self.receiver.clone()
    }

    pub(super) fn requested(&self) -> Option<ShutdownSignal> {
        *self.receiver.borrow()
    }

    pub(super) async fn wait(&mut self) -> BootResult<ShutdownSignal> {
        loop {
            if let Some(signal) = self.requested() {
                return Ok(signal);
            }
            self.receiver
                .changed()
                .await
                .map_err(|_| "shutdown coordinator stopped before producing a signal")?;
        }
    }
}

pub(super) fn handle_storage_lease_failure(runtime: &Runtime, trigger: &ShutdownTrigger) {
    tracing::error!("Midge writer lease became unhealthy; withdrawing readiness and shutting down");
    runtime.mark_storage_unavailable();
    trigger.request(ShutdownSignal::StorageLeaseFailure);
    runtime.begin_drain();
}

impl Drop for ShutdownCoordinator {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[cfg(unix)]
fn start_os_signal_task(trigger: ShutdownTrigger) -> BootResult<JoinHandle<()>> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(tokio::spawn(async move {
        loop {
            let (signal, monitor_closed) = tokio::select! {
                received = interrupt.recv() => {
                    if received.is_none() {
                        tracing::error!("SIGINT monitor closed unexpectedly; shutting down");
                    }
                    (ShutdownSignal::CtrlC, received.is_none())
                }
                received = terminate.recv() => {
                    if received.is_none() {
                        tracing::error!("SIGTERM monitor closed unexpectedly; shutting down");
                    }
                    (ShutdownSignal::Sigterm, received.is_none())
                }
            };
            trigger.request(signal);
            if monitor_closed || matches!(signal, ShutdownSignal::CtrlC) {
                return;
            }
        }
    }))
}

#[cfg(not(unix))]
fn start_os_signal_task(trigger: ShutdownTrigger) -> BootResult<JoinHandle<()>> {
    Ok(tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "shutdown signal monitor failed; shutting down");
        }
        trigger.request(ShutdownSignal::CtrlC);
    }))
}
