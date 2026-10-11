/// Fixed accepted-message envelopes for the Queue drain diagnostic workloads.
#[derive(Clone, Copy)]
pub(crate) struct Backlog(u64);

impl Backlog {
    pub(crate) const TEN_THOUSAND: Self = Self(10_000);
    pub(crate) const TWENTY_FIVE_THOUSAND: Self = Self(25_000);
    pub(crate) const HUNDRED_THOUSAND: Self = Self(100_000);

    pub(crate) const fn messages(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Envelope {
    FullWindow(Backlog),
    FiniteDrain(Backlog),
}

#[derive(Clone, Copy)]
pub(crate) enum Termination {
    ConfiguredWindow,
    BacklogGuard,
    AccountingGuard,
    EnqueueRejection,
    Failed,
}

impl From<&str> for Termination {
    fn from(value: &str) -> Self {
        match value {
            "configured_window" => Self::ConfiguredWindow,
            "backlog_safety_guard" => Self::BacklogGuard,
            "accounting_safety_guard" => Self::AccountingGuard,
            "broker_enqueue_rejection" => Self::EnqueueRejection,
            _ => Self::Failed,
        }
    }
}

/// Observed outcomes from the real pressure driver, independent of serialization.
pub(crate) struct ObservedStage {
    pub termination: Termination,
    pub configured_window_completed: bool,
    pub active_elapsed_ns: u128,
    pub offered: u64,
    pub harness_missed: u64,
    pub sent: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub acknowledged: u64,
    pub accepted_unacknowledged: u64,
    pub acknowledged_timing_samples: [u64; 5],
    pub minimum_pause_ns: Option<u128>,
    pub minimum_worker_pause_ns: Option<u128>,
    pub drain: DrainEvidence,
}

pub(crate) struct DrainEvidence {
    pub drained: bool,
    pub empty_verified: bool,
    pub clients_closed: bool,
}

impl Envelope {
    pub(crate) const WINDOW_SECONDS: u64 = 120;
    pub(crate) const PRODUCERS: usize = 256;
    pub(crate) const CONSUMER_DELAY_MS: u64 = 5;
    pub(crate) const MAX_ATTEMPTS: usize = 500_000;
    pub(crate) const DRAIN_SECONDS: u64 = 600;

    pub(crate) const fn backlog(self) -> u64 {
        match self {
            Self::FullWindow(backlog) | Self::FiniteDrain(backlog) => backlog.messages(),
        }
    }

    pub(crate) const fn rate(self) -> u64 {
        match self {
            // Even a stalled consumer cannot reach the guard during this window.
            Self::FullWindow(_) => self.backlog() * 4 / (5 * Self::WINDOW_SECONDS),
            Self::FiniteDrain(_) => 16_000,
        }
    }

    pub(crate) const fn scope(self) -> &'static str {
        match self {
            Self::FullWindow(_) => "completed_window_drain",
            Self::FiniteDrain(_) => "finite_accepted_message_drain",
        }
    }

    pub(crate) fn verify(self, stage: &ObservedStage) -> Result<(), &'static str> {
        if !stage.drain.drained || !stage.drain.empty_verified || !stage.drain.clients_closed {
            return Err("Queue drain, empty verification, or client cleanup failed");
        }
        if u128::from(stage.sent) != u128::from(stage.accepted) + u128::from(stage.rejected)
            || stage.accepted != stage.acknowledged
            || stage.accepted_unacknowledged != 0
        {
            return Err("Queue enqueue and acknowledgement accounting did not reconcile");
        }
        if u128::from(stage.offered) != u128::from(stage.sent) + u128::from(stage.harness_missed) {
            return Err("Queue offered arrivals did not reconcile with dispatched work");
        }
        if stage
            .acknowledged_timing_samples
            .iter()
            .any(|count| *count != stage.acknowledged)
        {
            return Err("Queue acknowledged work is missing consumer timing evidence");
        }
        let minimum_pause = u128::from(Self::CONSUMER_DELAY_MS) * 1_000_000;
        if stage
            .minimum_pause_ns
            .is_none_or(|pause| pause < minimum_pause)
            || stage
                .minimum_worker_pause_ns
                .is_none_or(|pause| pause < minimum_pause)
        {
            return Err("Queue consumer did not preserve the minimum pause after ACK");
        }
        match self {
            Self::FullWindow(_) => {
                if !matches!(stage.termination, Termination::ConfiguredWindow)
                    || !stage.configured_window_completed
                    || stage.active_elapsed_ns < u128::from(Self::WINDOW_SECONDS) * 1_000_000_000
                {
                    return Err("Queue full-window workload stopped before its configured window");
                }
                if u128::from(stage.accepted) * 100
                    < u128::from(self.rate()) * u128::from(Self::WINDOW_SECONDS) * 99
                {
                    return Err("Queue accepted work fell below the full-window envelope");
                }
            }
            Self::FiniteDrain(_) => {
                if matches!(stage.termination, Termination::Failed) {
                    return Err("Queue finite drain stopped with a failed workload");
                }
                if stage.accepted < self.backlog() {
                    return Err("Queue finite drain stopped below its accepted-message envelope");
                }
            }
        }
        if matches!(stage.termination, Termination::ConfiguredWindow)
            && u128::from(stage.harness_missed) * 100 > u128::from(stage.offered)
        {
            return Err("Queue workload missed more than one percent of offered arrivals");
        }
        Ok(())
    }
}
